#' @include MonetDBConnection.R MonetDBResult.R

#' @title RSQLite Compatibility Functions
#' @description
#' Dummy implementations of functions used by RSQLite but not MonetDB so as to
#' avoid unexpected errors for users who migrate from RSQLite to MonetDB.
#'
#' @param dbObj The database object, a `MonetDBResult` or `MonetDBConnection`.
#' @param ... Any other parameters. Currently, none is supported.
#' @export
#' @rdname RSQLite-compatibility
setGeneric("isIdCurrent", function(dbObj, ...) standardGeneric("isIdCurrent"))

#' @export
#' @rdname RSQLite-compatibility
setMethod(
  "isIdCurrent", signature(dbObj = "MonetDBResult"),
  function(dbObj, ...) {
    .Deprecated("dbIsValid")
    dbIsValid(dbObj)
  }
)

#' @export
#' @rdname RSQLite-compatibility
setMethod(
  "isIdCurrent", signature(dbObj = "MonetDBConnection"),
  function(dbObj, ...) {
    .Deprecated("dbIsValid")
    dbIsValid(dbObj)
  }
)

#' @export
#' @rdname RSQLite-compatibility
setGeneric(
  "initExtension",
  function(dbObj, ...) standardGeneric("initExtension")
)

#' @export
#' @rdname RSQLite-compatibility
setMethod(
  "initExtension", signature(dbObj = "MonetDBConnection"),
  function(dbObj, ...) {
    .Deprecated(msg = "initExtension() is not required for MonetDB")
  }
)
