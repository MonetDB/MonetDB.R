#' Check if the default MonetDB database is available.
#'
#' MonetDB examples and tests connect to a default database via
#' `dbConnect(`[MonetDB.R::MonetDB()]`)`. This function checks if that
#' database is available, and if not, displays an informative message.
#'
#' @param ... Additional arguments passed on to [dbConnect()]
#' @export
#' @examples
#' if (foundDefaultMonetDBdatabase()) {
#'   db <- monetdbDefault()
#'   dbListTables(db)
#'   dbDisconnect(db)
#' }
#' @rdname default
foundDefaultMonetDBdatabase <- function(...) {
  tryCatch({
    conn <- connect_default(...)
    dbDisconnect(conn)
    TRUE
  }, error = function(...) {
    message(
      "Could not fine the default MonetDB database.\n",
      "If MonetDB is running, check that it is serving the default database\n",
      "`demo` on `localhost:50001`\n"
    )
    FALSE
  })
}

#' @description
#' `monetdbDefault()` works similarly as `foundDefaultMonetDBdatabase()` but
#' returns a connection on success and throws a testthat skip condition on
#' failure, making it suitable for use in tests.
#' @export
#' @rdname default
monetdbDefault <- function(...) {
  tryCatch({
    connect_default(...)
  }, error = function(...) {
    testthat::skip("Default database not available")
  })
}

connect_default <- function(...) {
  dbConnect(MonetDB(), ...)
}
