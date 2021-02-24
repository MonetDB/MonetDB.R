#' Check if the default MonetDB database is available.
#'
#' MonetDB examples and tests connect to a default database via
#' `dbConnect(`[MonetDB.R::MonetDB()]`)`. This function checks if that
#' database is available, and if not, displays an informative message.
#'
#' @export
#' @examples
#' library(DBI)
#' if (foundDefaultMonetDBdatabase()) {
#'   conn <- monetdbDefault()
#'   dbWriteTable(conn, "mtcars", mtcars)
#'   dbReadTable(conn, "mtcars")
#'   dbRemoveTable(conn, "mtcars")
#'   dbListTables(conn)
#'   dbDisconnect(conn)
#' }
#' @rdname default
foundDefaultMonetDBdatabase <- function() {
  tryCatch(
    {
      conn <- dbConnect(MonetDB.R())
      dbDisconnect(conn)
      TRUE
    },
    error = function(...) {
      message(
        "Could not find the default MonetDB database.\n",
        "If MonetDB is running, check that it is serving the default database\n",
        "`demo` on `localhost:50001`\n"
      )
      FALSE
    }
  )
}

#' @description
#' `monetdbDefault()` works similarly as `foundDefaultMonetDBdatabase()` but
#' returns a connection on success and throws a testthat skip condition on
#' failure, making it suitable for use in tests.
#' @export
#' @rdname default
monetdbDefault <- function() {
  tryCatch(
    {
      dbConnect(MonetDB.R())
    },
    error = function(...) {
      testthat::skip("Default database not available")
    }
  )
}
