s3copy_format_times <- function(df, subsecond_digits = 0, with_tz_offset = TRUE) {
    time_strftime_fmt <- paste0(c("%Y-%m-%dT%H:%M:%OS",
                                  sprintf("%d", subsecond_digits),
                                  if(with_tz_offset) "%z" else ""),
                                collapse = "")
    df %>% dplyr::mutate_if(lubridate::is.POSIXt, strftime, time_strftime_fmt)
}


s3copy_format_dates <- function(df) {
    date_strftime_fmt <- "%Y-%m-%d"
    df %>% dplyr::mutate_if(lubridate::is.Date, strftime, date_strftime_fmt)
}


s3copy_execute <- function(redshift_conn,
                           s3_bucket, s3_key, iam_role_arn,
                           schema_name, table_name,
                           gz = FALSE) {
    copy_statement <- s3copy_sql_statement(redshift_conn, s3_bucket, s3_key, iam_role_arn, schema_name, table_name, gz)
    flog.debug("executing Redshift COPY statement:\n%s", copy_statement)
    n_rows_affected <- DBI::dbExecute(redshift_conn, copy_statement)
    return(invisible(NULL))
}


s3copy_sql_statement <- function(redshift_conn,
                                 s3_bucket, s3_key, iam_role_arn,
                                 schema_name, table_name,
                                 gz = FALSE) {
    sql_sprintf_args <- list(
        dest = if(is.null(schema_name)) {
                   DBI::dbQuoteIdentifier(redshift_conn, table_name)
               } else {
                   sprintf("%s.%s", DBI::dbQuoteIdentifier(redshift_conn, schema_name), DBI::dbQuoteIdentifier(redshift_conn, table_name))
               }
       ,s3uri = DBI::dbQuoteString(redshift_conn, sprintf("s3://%s/%s", s3_bucket, s3_key))
       ,iam = DBI::dbQuoteString(redshift_conn, iam_role_arn)
       ,gzip = if(gz) "GZIP" else ""
    )

    copy_statement <- with(sql_sprintf_args, glue::glue("
      COPY {dest}
      FROM {s3uri}
      IAM_ROLE {iam}
      FORMAT CSV
        {gzip} --- GZIP or nothing
        EMPTYASNULL
        ENCODING UTF8
        IGNOREBLANKLINES
        IGNOREHEADER 1
      COMPUPDATE TRUE
      MAXERROR 0
      --NOLOAD --- dry-run
      --STATUPDATE TRUE --- must be table owner to ANALYZE
    ")) %>% stringr::str_trim()

    return(copy_statement)
}


copy_table_to_redshift <- function(df, redshift_conn, schema_name, table_name, iam_role_arn,
                                   s3_bucket = "sadlab", s3_key_prefix = "tmp",
                                   subsecond_digits = 0, with_tz_offset = TRUE, gz = TRUE) {
    local_filepath <- if(gz) tempfile(fileext = ".gz") else tempfile()
    s3_key <- glue::glue("{s3_key_prefix}/{uuid::UUIDgenerate()}")
    flog.debug("table -> s3://%s/%s -> redshift/%s/%s", s3_bucket, s3_key, schema_name, table_name)
    df <- df %>%
        s3copy_format_times(subsecond_digits, with_tz_offset) %>%
        s3copy_format_dates()
    flog.debug(zzz::sstr(df, .name = "table"))
    s3io::s3io_write(df, s3_bucket, s3_key, readr::write_csv, localfile = local_filepath)
    do.call(s3copy_execute, tibble::lst(redshift_conn, s3_bucket, s3_key, iam_role_arn, schema_name, table_name, gz))
    awscli::s3api_delete_object(s3_bucket, s3_key)
    return(invisible(NULL))
}
