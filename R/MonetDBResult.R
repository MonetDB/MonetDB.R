#' @include MonetDBConnection.R
NULL

#' MonetDBResult and methods.
#'
#' @slot env list of connection environment variables and their values.
#' @export
setClass("MonetDBResult",
  contains = "DBIResult",
  slots = c(
    env = "environment"
  )
)

.CT_INT <- 0L
.CT_NUM <- 1L
.CT_CHR <- 2L
.CT_BOOL <- 3L
.CT_RAW <- 4L

# type mapping matrix
monetTypes <- rep(c(
  "integer", "numeric", "character", "character", "logical",
  "raw"
), c(5, 6, 4, 6, 1, 1))
names(monetTypes) <- c(
  # month_interval is the difference between date cols and int
  c("WRD", "TINYINT", "SMALLINT", "INT", "MONTH_INTERVAL"),
  # sec_interval is the difference between timestamps and float
  c("BIGINT", "HUGEINT", "REAL", "DOUBLE", "DECIMAL", "SEC_INTERVAL"),
  c("CHAR", "VARCHAR", "CLOB", "STR"),
  c("INTERVAL", "DATE", "TIME", "TIMETZ", "TIMESTAMP", "TIMESTAMPTZ"),
  c("BOOLEAN"),
  c("BLOB")
)

monetdbRtype <- function(dbType) {
  dbType <- toupper(dbType)
  rtype <- monetTypes[dbType]
  if (is.na(rtype)) {
    stop("Unknown DB type ", dbType)
  }
  rtype
}

# FIXME: check if this is still the case
#' @export
#' @rdname MonetDBResult-class
setMethod("fetch", signature(res = "MonetDBResult", n = "numeric"),
  function(res, n, ...) {
    # DBI on CRAN still uses fetch()
    # .Deprecated("dbFetch")
    dbFetch(res, n, ...)
  }
)

### DBIResult-class defined methods ###

# dbBind()

# dbClearResult()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbClearResult", "MonetDBResult", function(res, ...) {
  if (res@env$info$type == Q_TABLE) {
    resid <- res@env$info$id
    if (!is.null(resid) && !is.na(resid) && is.numeric(resid)) {
      .mapiRequest(res@env$conn, paste0("Xclose ", resid), async = TRUE)
      res@env$open <- FALSE
    }
  }
  return(invisible(TRUE))
}, valueClass = "logical")

# dbColumnInfo()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbColumnInfo", "MonetDBResult", function(res, ...) {
  data.frame(
    field.name = res@env$info$names, field.type = res@env$info$types,
    data.type = monetTypes[res@env$info$types],
    r.data.type = monetTypes[res@env$info$types],
    monetdb.data.type = res@env$info$types, stringsAsFactors = F
  )
},
valueClass = "data.frame"
)

# dbFetch()
# most of the heavy lifting here
#' @export
#' @rdname MonetDBResult-class
setMethod("dbFetch", signature(res = "MonetDBResult", n = "numeric"),
  function(res, n, ...) {
    if (!res@env$success) {
      stop(
        "Cannot fetch results from error response, error was ",
        res@env$info$message
      )
    }
    if (!dbIsValid(res)) {
      stop("Cannot fetch results from closed response.")
    }

    # okay, so we arrive here with the tuples from the first result in
    # res@env$data as a list
    info <- res@env$info
    # apparently, one should be able to fetch results sets from ddl ops
    if (info$type == Q_UPDATE) {
      return(data.frame())
    }
    if (res@env$delivered < 0) {
      res@env$delivered <- 0
    }
    stopifnot(res@env$delivered <= info$rows, info$index <= info$rows)
    remaining <- info$rows - res@env$delivered

    if (n < 0) {
      n <- remaining
    } else {
      n <- min(n, remaining)
    }

    # prepare the result holder df with columns of the appropriate type
    df <- list()
    ct <- rep(0L, info$cols)

    for (i in seq.int(info$cols)) {
      rtype <- monetdbRtype(info$types[i])
      if (rtype == "integer") {
        df[[i]] <- integer()
        ct[i] <- .CT_INT
      }
      if (rtype == "numeric") {
        df[[i]] <- numeric()
        ct[i] <- .CT_NUM
      }
      if (rtype == "character") {
        df[[i]] <- character()
        ct[i] <- .CT_CHR
      }
      if (rtype == "logical") {
        df[[i]] <- logical()
        ct[i] <- .CT_BOOL
      }
      if (rtype == "raw") {
        df[[i]] <- raw()
        ct[i] <- .CT_RAW
      }
      names(df)[i] <- info$names[i]
    }

    # we have delivered everything, return empty df (spec is not clear on this
    # one...)
    if (n < 1) {
      return(data.frame(df, stringsAsFactors = F))
    }

    # if our tuple cache in res@env$data does not contain n rows,
    # we fetch from server until it does
    while (length(res@env$data) < n) {
      cresp <- .mapiParseResponse(.mapiRequest(res@env$conn, paste0(
        "Xexport ", .mapiLongInt(info$id),
        " ", .mapiLongInt(info$index), " ",
        .mapiLongInt(n - length(res@env$data))
      )))
      stopifnot(cresp$type == Q_BLOCK && cresp$rows > 0)

      res@env$data <- c(res@env$data, cresp$tuples)
      info$index <- info$index + cresp$rows
      # if (getOption("monetdb.profile", T)) {
      #  .profiler_progress(length(res@env$data), n)
      # }
    }

    # convert tuple string vector into matrix so we can access a single column
    # efficiently call to a faster C implementation for the annoying task of
    # splitting everyting into fields
    parts <- .Call("mapi_split", res@env$data[1:n], as.integer(info$cols),
      PACKAGE = C_LIBRARY
    )

    # convert values column by column
    for (j in seq.int(info$cols)) {
      col <- ct[[j]]
      if (col == .CT_INT) {
        df[[j]] <- as.integer(parts[[j]])
      }
      if (col == .CT_NUM) {
        df[[j]] <- as.numeric(parts[[j]])
      }
      if (col == .CT_BOOL) {
        df[[j]] <- parts[[j]] == "true"
      }
      if (col == .CT_CHR) {
        df[[j]] <- parts[[j]]
        Encoding(df[[j]]) <- "UTF-8"
      }
      if (col == .CT_RAW) {
        df[[j]] <- lapply(parts[[j]], charToRaw)
      }
    }

    # remove the already delivered tuples from the background holder or clear
    # it altogether
    if (n + 1 >= length(res@env$data)) {
      res@env$data <- character()
    }
    else {
      res@env$data <- res@env$data[seq(n + 1, length(res@env$data))]
    }
    res@env$delivered <- res@env$delivered + n

    # this is a trick so we do not have to call data.frame(), which is expensive
    attr(df, "row.names") <- c(NA_integer_, length(df[[1]]))
    class(df) <- "data.frame"

    # if (getOption("monetdb.profile", T))  .profiler_clear()
    df
  }
)

# dbGetInfo()

# dbGetRowCount()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbGetRowCount", "MonetDBResult", function(res, ...) {
  res@env$info$rows
},
valueClass = "numeric"
)

# dbGetRowsAffected()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbGetRowsAffected", "MonetDBResult", function(res, ...) {
  as.numeric(NA)
},
valueClass = "numeric"
)

# dbGetStatement()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbGetStatement", "MonetDBResult", function(res, ...) {
  res@env$query
},
valueClass = "character"
)

# dbHasCompleted()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbHasCompleted", "MonetDBResult", function(res, ...) {
  if (res@env$info$type == Q_TABLE) {
    return(res@env$delivered == res@env$info$rows)
  }
  return(invisible(TRUE))
}, valueClass = "logical")

# dbIsReadOnly()

# dbIsValid()
#' @export
#' @rdname MonetDBResult-class
setMethod("dbIsValid", signature(dbObj = "MonetDBResult"),
          function(dbObj, ...) {
            if (dbObj@env$info$type == Q_TABLE) {
              return(dbObj@env$open)
            }
            return(invisible(TRUE))
          }
)

# dbQuoteIdentifier()

# dbQuoteLiteral()

# dbQuoteString()

# dbUnquoteIdentifier()


### compatibility with RSQLite ###
#' @name RSQLite-compatibility
#' @title RSQLite Compatibility Functions
#' @description
#' Functions for RSQLite compatibility.
#' @param dbObj The database object, a `MonetDBResult` or `MonetDBConnection`.
#' @param ... Any other parameters. Currently, none is supported.
#' @seealso `RSQLite::isIdCurrent()` `RSQLite::initExtension()`
if (is.null(getGeneric("isIdCurrent"))) {
  setGeneric("isIdCurrent", function(dbObj, ...) standardGeneric("isIdCurrent"))
}
#' @export
#' @rdname RSQLite-compatibility
setMethod("isIdCurrent", signature(dbObj = "MonetDBResult"),
          function(dbObj, ...) {
    .Deprecated("dbIsValid")
    dbIsValid(dbObj)
  }
)

#' Functions for RSQLite compatibility.
#'
#' @param dbObj A `MonetDBConnection` object.
#' @export
#' @rdname RSQLite-compatibility
setMethod("isIdCurrent", signature(dbObj = "MonetDBConnection"),
  function(dbObj, ...) {
    .Deprecated("dbIsValid")
    dbIsValid(dbObj)
  }
)

if (is.null(getGeneric("initExtension"))) {
  setGeneric(
    "initExtension",
    function(dbObj, ...) standardGeneric("initExtension")
  )
}
#' Functions for RSQLite compatibility.
#'
#' @param dbObj A `MonetDBConnection` object.
#' @export
#' @rdname RSQLite-compatibility
setMethod("initExtension", signature(dbObj = "MonetDBConnection"),
  function(dbObj, ...) {
    .Deprecated(msg = "initExtension() is not required for MonetDB")
  }
)

### monetdb.read.csv ###
#' @name monetdb.read.csv
#' @title monet.read.csv
#' @description
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
#'
#' @examples
#' library(DBI)
#' conn <- dbConnect(MonetDB.R::MonetDB(), dbname = "demo")
#' file <- tempfile()
#' write.table(iris, file, sep = ",", row.names = F)
#' MonetDB.R::monetdb.read.csv(conn, file, "iris")
#'
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
        dbCreateTable(
          conn, tablename,
          read.csv(files[1], header = header, sep = delim)
        )
      }
      else {
        dbCreateTable(
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
