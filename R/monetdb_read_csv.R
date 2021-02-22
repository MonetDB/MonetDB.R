#' monetdb.read.csv
#'
#' Instruct MonetDB to read a CSV file, optionally also create the table for it.
#' Note that this causes MonetDB to read a file on the machine where the server
#' is running, not on the machine where the R client runs.
#'
#' @param conn
#'        A MonetDB.R database connection, created using [DBI::dbConnect] with
#'        the [MonetDB.R] database driver.
#' @param files
#'        A single string or a vector of strings containing the absolute file
#'        names of the CSV files to be imported.
#' @param tablename
#'        The dataframe that needs to be stored in the table
#' @param header
#'        Whether or not the CSV files contain a header line.
#' @param best.effort
#'        Use best effort flag when reading csv files and continue importing
#'        even if parsing of fields/lines fails.
#' @param delim
#'        Field separator in CSV file.
#' @param newline
#'        Newline in CSV file, usually `\\n` for UNIX-like systems and
#'        `\\r\\r` on Windows.
#' @param quote
#'        Quote character(s) in CSV file.
#' @param create
#'        Create table before importing?
#' @param col.names
#'        Optional column names in case the ones from CSV file should not be
#'        used
#' @param lower.case.names
#'        Convert all column names to lowercase in the database?
#' @param sep
#'        Alias for `delim`
#' @param ...
#'        Any other parameters. Ignored.
#' @return Returns the number of rows imported if successful.
#' @seealso [utils::read.csv]
#'
#' @examples
#' library(DBI)
#' conn <- dbConnect(MonetDB.R::MonetDB(), dbname = "demo")
#' file <- tempfile()
#' write.table(iris, file, sep = ",", row.names = F)
#' MonetDB.R::monetdb.read.csv(conn, file, "iris")
#'
#' @name monetdb.read.csv
#' @include MonetDBConnection.R MonetDBResult.R
#' @importFrom utils read.csv write.table
NULL

#' @export
monetdb.read.csv <-
  function(conn, files, tablename, header = TRUE, best.effort = FALSE,
           delim = ",", newline = "\\n", quote = "\"", create = TRUE,
           col.names = NULL, lower.case.names = FALSE, sep = delim, ...) {
    if (!missing(sep)) delim <- sep

    dbBegin(conn)
    on.exit(tryCatch(dbRollback(conn), error = function(e) {}))

    # Create the table.
    if (create) {
      if (length(files) > 1) {
        DBI::dbCreateTable(
          conn, tablename,
          read.csv(files[1], header = header, sep = delim)
        )
      }
      else {
        DBI::dbCreateTable(
          conn, tablename,
          read.csv(files, header = header, sep = delim)
        )
      }
    }

    query <- paste0("COPY OFFSET 2 INTO ", tablename, " FROM ")

    # Loop for multi file support.
    for (i in seq_along(files)) {
      query <- paste0(query, "'", files[i], "'")

      if (i >= length(files)) {
        break
      }

      query <- paste0(query, ",")
    }

    query <- paste0(
      query, " DELIMITERS '", delim, "'",
      if (best.effort) " BEST EFFORT", ";"
    )
    dbSendQuery(conn, query)

    dbCommit(conn)
    on.exit(NULL)
  }
#' @export
#' @rdname monetdb.read.csv
monet.read.csv <- monetdb.read.csv
