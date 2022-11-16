#' @importFrom magrittr %>%
NULL

try_silent <- function(expr) {
  try(expr, silent = TRUE)
}


format_keep_na <- function(f) {
  force(f)
  function(x, ...) {
    na_lix <- is.na(x)
    retval <- f(x, ...)
    retval[na_lix] <- NA  ## type-coercion
    retval
  }
}


iso8601 <- function(x,
                    what = c("datetime", "timestamp", "date", "time"), 
                    tz = NULL,
                    tz_offset = TRUE,
                    digits = 0) {
  ## "datetime" and "timestamp" are equivalent as far as I'm concerned.
  ## (A timestamp without the date is simple a "time".)
  what <- match.arg(what)
  if (what == "datetime") what <- "timestamp"

  if (is.null(tz)) tz <- ""
  checkmate::assert_string(tz, null.ok = TRUE)
  checkmate::assert_subset(tz, c("", OlsonNames()), empty.ok = FALSE)

  digits <- as.integer(digits)
  date_pattern <- "%Y-%m-%d"
  time_pattern <- paste("%H:%M:%OS", digits, sep = "")
  tz_pattern <- if (tz_offset) "%z" else ""

  strftime_pattern <-
    if (what == "timestamp") {
      sprintf("%sT%s%s", date_pattern, time_pattern, tz_pattern)
    } else if (what == "date") {
      date_pattern
    } else if (what == "time") {
      sprintf("%s%s", time_pattern, tz_pattern)
    } else {
      stop(sprintf("unrecognized `what` = %s", what))
    }

  strftime(x, strftime_pattern, tz = tz, usetz = FALSE)
}


format_datetime <- format_keep_na(function(x, time_tz = NULL, time_tz_offset = TRUE, time_digits = 6) {
  iso8601(lubridate::as_datetime(x), what = "datetime", tz = time_tz, tz_offset = time_tz_offset, digits = time_digits)
})


format_date <- format_keep_na(function(x) {
  iso8601(lubridate::as_date(x), what = "date")
})


format_numeric <- format_keep_na(function(x) {
  format(as.numeric(x), digits = NULL, scientific = FALSE, trim = TRUE, drop0trailing = TRUE)
})


redshift_copy_sql_statement <- function(s3_bucket, s3_key,
                                        iam_role_arn,
                                        schema_name, table_name,
                                        gz = FALSE) {
  gz <- DBI::SQL(if (isTRUE(gz)) "GZIP" else "")
  s3_uri <- glue::glue("s3://{s3_bucket}/{s3_key}")
  glue::glue_sql("
    COPY {`schema_name`}.{`table_name`}
    FROM {s3_uri}
    IAM_ROLE {iam_role_arn}
    FORMAT CSV
      {gz}
      EMPTYASNULL
      ENCODING UTF8
      IGNOREBLANKLINES
      IGNOREHEADER 1
      TIMEFORMAT 'auto'
    COMPUPDATE TRUE
    MAXERROR 0
    --NOLOAD --- dry-run
    --STATUPDATE TRUE --- must be table owner to ANALYZE
  ", .con = DBI::ANSI())
}


redshift_exec_copy <- function(redshift_conn,
                               s3_bucket, s3_key,
                               iam_role_arn,
                               schema_name, table_name,
                               gz = FALSE) {
  DBI::dbExecute(redshift_conn, redshift_copy_sql_statement(s3_bucket, s3_key, iam_role_arn, schema_name, table_name, gz))
}


#' @export
s3copy <- function(tbl,
                   redshift_conn,
                   schema_name, table_name,
                   iam_role_arn,
                   s3_bucket, s3_key, s3_delete_on_success = TRUE,
                   time_tz = NULL, time_tz_offset = TRUE, time_digits = 6,
                   gz = TRUE,
                   .aws_opts = NULL) {
  gz <- isTRUE(gz)
  local_filepath <- if (gz) tempfile(fileext = ".gz") else tempfile()

  tbl <- tbl %>%
    dplyr::mutate(across(where(lubridate::is.POSIXt), format_datetime)) %>%
    dplyr::mutate(across(where(lubridate::is.Date), format_date)) %>%
    dplyr::mutate(across(where(is.numeric), format_numeric))

  s3io::s3io_write(tbl, s3_bucket, s3_key, readr::write_csv, na = "", .localfile = local_filepath, .opts = .aws_opts)

  retval <- redshift_exec_copy(redshift_conn, s3_bucket, s3_key, iam_role_arn, schema_name, table_name, gz)

  if (isTRUE(s3_delete_on_success)) {
    try_silent(
      awscli2::awscli(
        c("s3api", "delete-object"),
        "--bucket" = s3_bucket,
        "--key" = s3_key,
        .config = .aws_opts
      )
    )
  }

  retval
}
