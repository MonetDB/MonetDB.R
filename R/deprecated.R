#' Deprecated functions
#'
#' Deprecated functions kept for backward compatibility purpose.
#'
#' @include MonetDBResult.R
#' @name MonetDB.R-deprecated
NULL

#' A wrapper for [dbFetch()]. Needed because DBI on CRAN still uses fetch()
#' @inheritParams DBI::dbFetch
#' @seealso [dbFetch()]
#' @export
#' @rdname MonetDB.R-deprecated
setMethod("fetch", signature(res = "MonetDBResult", n = "numeric"),
          function(res, n, ...) {
            # .Deprecated("dbFetch")
            dbFetch(res, n, ...)
          }
)
