convert_keep_na <- function(x, f, ...) {
    na_lix <- is.na(x)
    retval <- f(x, ...)
    retval[na_lix] <- NA
    retval
}


## s3copy_sql_statement <- function(redshift_conn,
##                                  s3_bucket, s3_key, iam_role_arn,
##                                  schema_name, table_name,
##                                  gz = FALSE) {
##     sql_sprintf_args <- list(
##         dest = if(is.null(schema_name)) {
##                    DBI::dbQuoteIdentifier(redshift_conn, table_name)
##                } else {
##                    sprintf("%s.%s", DBI::dbQuoteIdentifier(redshift_conn, schema_name), DBI::dbQuoteIdentifier(redshift_conn, table_name))
##                }
##        ,s3uri = DBI::dbQuoteString(redshift_conn, sprintf("s3://%s/%s", s3_bucket, s3_key))
##        ,iam = DBI::dbQuoteString(redshift_conn, iam_role_arn)
##        ,gzip = if(gz) "GZIP" else ""
##     )

##     copy_statement <- with(sql_sprintf_args, glue::glue("
##       COPY {dest}
##       FROM {s3uri}
##       IAM_ROLE {iam}
##       FORMAT CSV
##         {gzip} --- GZIP or nothing
##         EMPTYASNULL
##         ENCODING UTF8
##         IGNOREBLANKLINES
##         IGNOREHEADER 1
##         TIMEFORMAT 'auto'
##       COMPUPDATE TRUE
##       MAXERROR 0
##       --NOLOAD --- dry-run
##       --STATUPDATE TRUE --- must be table owner to ANALYZE
##     ")) %>% stringr::str_trim()

##     return(copy_statement)
## }


## s3copy_execute <- function(redshift_conn,
##                            s3_bucket, s3_key, iam_role_arn,
##                            schema_name, table_name,
##                            gz = FALSE) {
##     copy_statement <- s3copy_sql_statement(redshift_conn, s3_bucket, s3_key, iam_role_arn, schema_name, table_name, gz)
##     flog.debug("executing Redshift COPY statement:\n%s", copy_statement)
##     n_rows_affected <- DBI::dbExecute(redshift_conn, copy_statement)
##     return(invisible(NULL))
## }

#' @title Load data from R data frame to Redshift via a COPY (from S3) command.
s3copy <- function(df,
                   redshift_conn, schema_name, table_name, iam_role_arn,
                   s3_bucket, s3_key, s3_delete_on_success = TRUE,
                   time_tz = NULL, time_tz_offset = TRUE, time_digits = 6,
                   gz = TRUE)
{
    local_filepath <- if(gz) tempfile(fileext = ".gz") else tempfile()
    df <- df %>%
        dplyr::mutate_if(lubridate::is.POSIXt, convert_keep_na, f = zzz::iso8601,
                         what = "datetime", tz = time_tz, tz_offset = time_tz_offset, digits = time_digits) %>%
        dplyr::mutate_if(lubridate::is.Date, convert_keep_na, f = zzz::iso8601,
                         what = "date") %>%
        dplyr::mutate_if(is.numeric, convert_keep_na, f = format,
                         digits = NULL, scientific = FALSE, trim = TRUE, drop0trailing = TRUE)
    s3io::s3io_write(df, s3_bucket, s3_key, readr::write_csv, na = "", .localfile = local_filepath)
    do.call(s3copy_execute, tibble::lst(redshift_conn, s3_bucket, s3_key, iam_role_arn, schema_name, table_name, gz))
    if(s3_delete_on_success) awscli::s3api_delete_object(s3_bucket, s3_key)
    return(invisible(NULL))
}
