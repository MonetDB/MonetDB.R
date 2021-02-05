#' MonetDB driver
#'
#' @export
#' @aliases MonetR MonetDB MonetDBR
#' @useDynLib MonetDB.R, .registration = TRUE
#' @import methods DBI
MonetDB.R <- function() {
  new("MonetDBDriver")
}
# allow instantiation of this driver with MonetDB to allow existing programs
# to work
#' @export
MonetR <- MonetDB <- MonetDBR <- MonetDB.R

# C library that contains our MAPI string splitting state machine
C_LIBRARY <- "MonetDB.R"

#' MonetDBDriver and methods.
#'
#' @export
setClass("MonetDBDriver", contains = "DBIDriver")

# FIXME: implement a show() method for each? class

### DBIDriver-class defined methods ###

# dbDriver() # deprecated

# dbUnloadDriver() # not implemented for modern backends
#' @export
#' @rdname MonetDBDriver-class
setMethod("dbUnloadDriver", "MonetDBDriver", function(drv, ...) {
  # FIXME: or should we put "NULL" here?
  invisible(TRUE)
})

# dbConnect()

# dbCanConnect()

# dbListConnections() # deprecated

# dbDataType()

# dbIsValid()
#' @export
#' @rdname MonetDBDriver-class
setMethod("dbIsValid", "MonetDBDriver", function(dbObj, ...) {
  invisible(TRUE)
})

# dbGetInfo()
#' @export
#' @rdname MonetDBDriver-class
setMethod("dbGetInfo", "MonetDBDriver", function(dbObj, ...) {
  list(
    name = "MonetDBDriver",
    driver.version = utils::packageVersion("MonetDB.R"),
    DBI.version = utils::packageVersion("DBI"),
    # FIXME: try to set the client.version
    client.version = NA,
    # R can only handle 128 connections, three of which are pre-allocated
    max.connections = 125
  )
})
