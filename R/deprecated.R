#' @include MonetDBResult.R

#' @title Deprecated functions
#' @description
#' Deprecated functions are kept here for backward compatibility purpose.
#'
#' The function `fetch()` is a wrapper for [MonetDB.R::dbFetch()], which is
#' needed because DBI on CRAN still uses fetch()
#' @inheritParams DBI::dbFetch
#' @seealso [dbFetch()]
#' @export
#' @rdname deprecated
setMethod("fetch", signature(res = "MonetDBResult", n = "numeric"),
          function(res, n, ...) {
            # .Deprecated("dbFetch")
            dbFetch(res, n, ...)
          }
)
