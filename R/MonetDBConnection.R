#' @include MonetDBDriver.R
NULL

#' MonetDBConnection and methods.
#'
#' @export
#' @slot connenv list of connection environment variables and their values.
#' @keywords internal
setClass("MonetDBConnection",
  contains = "DBIConnection",
  slots = c(
    connenv = "environment"
  )
)

# quoting
quoteIfNeeded <- function(conn, x, warn = F, ...) {
  x <- as.character(x)
  chars <- !grepl("^[a-z_][a-z0-9_]*$", x, perl = T) &
    !grepl("^\"[^\"]*\"$", x, perl = T)
  if (any(chars) && (warn || getOption("monetdb.debug.query", F))) {
    message(
      "Identifier(s) ",
      paste("\"", x[chars], "\"", collapse = ", ", sep = ""),
      paste(
        " contain uppercase or reserved SQL characters and",
        "need(s) to be quoted in queries."
      )
    )
  }
  reserved <- toupper(x) %in% conn@connenv$keywords
  if (any(reserved) && (warn || getOption("monetdb.debug.query", F))) {
    message(
      "Identifier(s) ",
      paste("\"", x[reserved], "\"", collapse = ", ", sep = ""),
      " are reserved SQL keywords and need(s) to be quoted in queries."
    )
  }
  qts <- reserved | chars
  if (any(qts)) {
    x[qts] <- DBI::dbQuoteIdentifier(conn, x[qts])
  }
  x
}

.bindParameters <- function(statement, param) {
  for (i in 1:length(param)) {
    value <- param[[i]]
    valueClass <- class(value)
    if (isTRUE(is.na(value))) {
      statement <- sub("?", "NULL", statement, fixed = TRUE)
    } else if (isTRUE(valueClass %in% c("numeric", "logical", "integer") &&
      isTRUE(!is.null(value)))) {
      statement <- sub("?", value, statement, fixed = TRUE)
    } else if (isTRUE(valueClass == c("raw"))) {
      stop("raw() data is so far only supported when reading from BLOBs")
    } else {
      statement <- sub("?",
        paste("'", .mapiQuote(toString(value)), "'", sep = ""), statement,
        fixed = TRUE
      )
    }
  }
  return(statement)
}

# quote strings when sending them to the db. single quotes are most critical.
# null bytes are not supported
.mapiQuote <- function(str) {
  qs <- ""
  chrs <- unlist(strsplit(str, "", fixed = TRUE))
  for (chr in chrs) {
    f <- ""
    if (chr == "\n") f <- qs <- paste0(qs, "\\", "n")
    if (chr == "\t") f <- qs <- paste0(qs, "\\", "t")
    if (chr == "'") f <- qs <- paste0(qs, "\\'")
    if (nchar(f) == 0) qs <- paste0(qs, chr)
  }
  qs
}

### DBIConnection-class defined methods ###

# show()

# DBIDriver > dbDataType()
#' @title dbDataType
#' @description
#' Determine database type for an R object
#'
#' @param dbObj MonetDB driver or connection.
#' @param obj R Object to convert
#' @param ... Any other parameters. Currently, none is supported.
#'
#' @export
#' @rdname dbDataType
setMethod("dbDataType",
  signature(dbObj = "MonetDBConnection", obj = "ANY"),
  function(dbObj, obj, ...) {
    if (is.logical(obj)) {
      "BOOLEAN"
    } else if (is.integer(obj)) {
      "INTEGER"
    } else if (is.numeric(obj)) {
      "DOUBLE PRECISION"
    } else if (is.raw(obj)) {
      "BLOB"
    } else {
      "STRING"
    }
  },
  valueClass = "character"
)

# DBIdriver > dbConnect()
#' @title dbConnect
#' @description
#' `DBI::dbConnect()` establishes a connection to a database.
#' Set `drv = MonetDB.R::MonetDB()` to connect to a MonetDB database
#' using the \pkg{MonetDB.R} package.
#'
#' @param drv Should be set to [MonetDB.R::MonetDB()]
#'   to use the \pkg{MonetDB.R} package.
#' @param dbname The name of the database to connect. Default `"demo"`.
#' @param user The name of the database user to log in. Default `"monetdb"`.
#' @param password Password to use. Default `"monetdb"`.
#' @param host Hostname where the MonetDB server is running.
#'   Default `localhost`.
#' @param port Port number to which the MonetDB server is listening.
#'   Default: `50000`.
#' @param timeout Query timeout in seconds. Default: 86400.
#' @param wait If the server is not ready yet, wait for it? Default: no.
#' @param language The query language to use. Default: `"sql"`.
#' @param ... Other name-value pairs that describe additional connection
#'   options.
#' @param url `dbname` as a url string. Default: `""`.
#' @return conn Connection established to the given MonetDB server.
#'
#' @examples
#' library(DBI)
#' if (foundDefaultMonetDBdatabase()) {
#'   # Connect to the default database
#'   conn <- dbConnect(MonetDB.R::MonetDB())
#'   DBI::dbGetQuery(conn, "select 42")
#'   dbDisconnect(conn)
#'
#'   # Connect to a database with customised parameter values
#'   conn <- dbConnect(MonetDB.R::MonetDB(),
#'     dbname = "demo", user = "monetdb",
#'     password = "monetdb", host = "localhost", port = 50000L
#'   )
#'   DBI::dbGetQuery(conn, "select 43")
#'   dbDisconnect(conn)
#'
#'   # Connect to a database using a URL string
#'   conn <- dbConnect(MonetDB.R::MonetDB(), "monetdb://localhost:50000/demo")
#'   DBI::dbGetQuery(conn, "select 44")
#'   dbDisconnect(conn)
#' }
#' @export
#' @rdname MonetDB.R
setMethod("dbConnect", "MonetDBDriver",
  function(drv, dbname = "demo", user = "monetdb", password = "monetdb",
           host = "localhost", port = 50000L, timeout = 86400L, wait = FALSE,
           language = "sql", ..., url = "") {
    if (substring(url, 1, 10) == "monetdb://") {
      dbname <- url
    }
    timeout <- as.integer(timeout)

    if (substring(dbname, 1, 10) == "monetdb://") {
      rest <- substring(dbname, 11, nchar(dbname))
      # split at /, so we get the dbname
      slashsplit <- strsplit(rest, "/", fixed = TRUE)
      hostport <- slashsplit[[1]][1]
      dbname <- slashsplit[[1]][2]

      # count the number of : in the string
      ndc <- nchar(hostport) - nchar(gsub(":", "", hostport, fixed = T))
      if (ndc == 0) {
        host <- hostport
      }
      if (ndc == 1) { # ipv4 case, any ipv6 address has more than one :
        hostportsplit <- strsplit(hostport, ":", fixed = TRUE)
        host <- hostportsplit[[1]][1]
        port <- hostportsplit[[1]][2]
      }
      if (ndc > 1) { # ipv6 case, now we only need to check for ]:
        # ipv6 with port number
        if (length(grep("]:", hostport, fixed = TRUE)) == 1) {
          hostportsplit <- strsplit(hostport, "]:", fixed = TRUE)
          host <- substring(hostportsplit[[1]][1], 2)
          port <- hostportsplit[[1]][2]
        }
        else {
          host <- hostport
        }
      }
    }
    # this is important, otherwise we'll trip an assertion
    port <- as.integer(port)
    # validate port number
    if (length(port) != 1 || port < 1 || port > 65535) {
      stop("Illegal port number ", port)
    }

    if (getOption("monetdb.debug.mapi", F)) {
      message(
        "II: Connecting to MonetDB on host ", host, " at port ", port,
        " to DB ", dbname, " with user ", user,
        " and a non-printed password, timeout is ", timeout, " seconds."
      )
    }
    socket <- FALSE
    if (wait) {
      repeat {
        continue <- FALSE
        tryCatch(
          {
            # open socket with a 5-sec timeout
            # so we can check whether everything works
            suppressWarnings(socket <<- .mapiConnect(host, port, 5))
            # authenticate
            .mapiAuthenticate(socket, dbname, user, password,
              language = language
            )
            .mapiDisconnect(socket)
            break
          },
          error = function(e) {
            if ("connection" %in% class(socket)) {
              tryCatch(close(socket), error = function(e) {})
            }
            message(
              "Server not ready(", e$message,
              "), retrying (ESC or CTRL+C to abort)"
            )
            Sys.sleep(1)
            continue <<- TRUE
          }
        )
      }
    }

    # make new socket with user-specified timeout

    connenv <- new.env(parent = emptyenv())
    connenv$lock <- 0
    connenv$deferred <- list()
    connenv$exception <- list()
    connenv$params <- list(
      drv = drv, host = host, port = port, timeout = timeout,
      dbname = dbname, user = user, password = password,
      language = language
    )
    connenv$socket <- .mapiConnect(host, port, timeout)
    .mapiAuthenticate(connenv$socket, dbname, user, password,
      language = language
    )

    conn <- new("MonetDBConnection", connenv = connenv)

    # Fill the MonetDB keywords
    connenv$keywords <- array(c(unlist(
      DBI::dbGetQuery(
        conn,
        "SELECT DISTINCT * FROM sys.keywords ORDER BY keyword"
      )
    )))

    if (getOption("monetdb.sequential", F)) {
      message("MonetDB: Switching to single-threaded query execution.")
      dbSendQuery(conn, "set optimizer='sequential_pipe'")
    }
    attr(conn, "dbPreExists") <- TRUE
    conn
  },
  valueClass = "MonetDBConnection"
)

# dbDisconnect()
#' @param conn
#'   a connection to a MonetDB server established earlier with
#'   `MonetDB.R::dbConnect()`
#' @export
#' @rdname MonetDB.R
setMethod("dbDisconnect", "MonetDBConnection", function(conn, ...) {
  .mapiDisconnect(conn@connenv$socket)
  invisible(TRUE)
})

# dbSendQuery()
#' @title dbSendQuery
#' @description
#' FIXME: update this documentation!
#' Execute a SQL statement on a database connection. This one does all the work
#' in this class.
#'
#' To retrieve results a chunk at a time, use `dbSendQuery()`,
#' `dbFetch()`, then `dbClearResult()`. Alternatively, if you want all the
#' results (and they'll fit in memory) use `dbGetQuery()` which sends,
#' fetches and clears for you.
#'
#' @param conn A [MonetDBConnection-class] created by [dbConnect()].
#' @param statement An SQL string to execute
#' @param ... Another arguments needed for compatibility with generic (currently
#'   ignored).
#' @param list A list of extra parameters. Default: `NULL`.
#' @param async Execute the query in Async mode? Default: `FALSE`.
#'
#' @examples
#' library(DBI)
#' if (foundDefaultMonetDBdatabase()) {
#'   conn <- dbConnect(MonetDB.R())
#'   dbWriteTable(conn, "usarrests", datasets::USArrests, temporary = TRUE)
#'
#'   # Run query to get results as dataframe
#'   DBI::dbGetQuery(conn, "SELECT * FROM usarrests LIMIT 3")
#'
#'   # Send query to pull results in batches
#'   res <- dbSendQuery(conn, "SELECT * FROM usarrests")
#'   dbFetch(res, n = 2)
#'   dbFetch(res, n = 2)
#'   dbHasCompleted(res)
#'   dbClearResult(res)
#'   dbRemoveTable(conn, "usarrests")
#'   dbDisconnect(conn)
#' }
#' @export
#' @rdname dbSendQuery
setMethod(
  "dbSendQuery",
  signature(conn = "MonetDBConnection", statement = "character"),
  function(conn, statement, ..., list = NULL, async = FALSE) {
    if (!is.null(list) || length(list(...))) {
      if (length(list(...))) statement <- .bindParameters(statement, list(...))
      if (!is.null(list)) statement <- .bindParameters(statement, list)
    }
    conn@connenv$exception <- list()
    env <- NULL
    if (getOption("monetdb.debug.query", F)) {
      message("QQ: '", statement, "'")
    }
    log_file <- getOption("monetdb.log.query", NULL)
    if (!is.null(log_file)) {
      cat(c(statement, ";\n"), file = log_file, sep = "", append = TRUE)
    }
    # the actual request
    resp <- NA
    tryCatch(
      {
        mresp <- .mapiRequest(conn, paste0("s", statement, "\n;"),
          async = async
        )
        resp <- .mapiParseResponse(mresp)
      },
      interrupt = function(ex) {
        message("Interrupted query execution. Attempting to fix connection....")

        newconn <- do.call("dbConnect", conn@connenv$params)
        dbDisconnect(conn)
        conn@connenv$socket <- newconn@connenv$socket
        conn@connenv$lock <- 0
        conn@connenv$deferred <- list()
        conn@connenv$exception <- list()

        stop("No query result for now.")
      }
    )

    env <- new.env(parent = emptyenv())

    if (resp$type == Q_TABLE) {
      # we have to pass this as an environment to make conn object available to
      # result for fetching
      env$success <- TRUE
      env$conn <- conn
      env$data <- resp$tuples
      resp$tuples <- NULL # clean up
      env$info <- resp
      env$delivered <- -1
      env$query <- statement
      env$open <- TRUE
    }
    if (resp$type == Q_UPDATE || resp$type == Q_CREATE ||
      resp$type == MSG_ASYNC_REPLY || resp$type == MSG_PROMPT) {
      env$success <- TRUE
      env$conn <- conn
      env$query <- statement
      env$info <- resp
    }
    if (resp$type == MSG_MESSAGE) {
      env$success <- FALSE
      env$conn <- conn
      env$query <- statement
      env$info <- resp
      env$message <- resp$message
    }

    if (!env$success) {
      sp <- strsplit(env$message, "!", fixed = T)[[1]]
      # truncate statement to not hide actual error message
      if (nchar(statement) > 100) {
        statement <- paste0(substring(statement, 1, 100), "...")
      }
      if (length(sp) == 3) {
        errno <- sp[[2]]
        errmsg <- sp[[3]]
        conn@connenv$exception <- list(errNum = errno, errMsg = errmsg)
        stop(
          "Unable to execute statement '", statement, "'.\nServer says '",
          errmsg, "' [#", errno, "]."
        )
      }
      else {
        conn@connenv$exception <- list(errNum = NA, errMsg = env$message)
        stop(
          "Unable to execute statement '", statement, "'.\nServer says '",
          env$message, "'."
        )
      }
    }

    invisible(new("MonetDBResult", env = env))
  }
)

# dbSendStatement()

# dbGetQuery()

# dbExecute()

# dbGetException() # DEPRECATED
#' @export
#' @rdname MonetDBConnection-class
setMethod("dbGetException", "MonetDBConnection", function(conn, ...) {
  conn@connenv$exception
})

# dbListResults()

# dbListObjects()

### methods from DBIObjeect ###
# DBIObject > dbGetInfo()
#' @export
#' @rdname MonetDBConnection-class
setMethod("dbGetInfo", "MonetDBConnection", function(dbObj, ...) {
  envdata <- DBI::dbGetQuery(dbObj, "SELECT name, value FROM sys.env()")
  ll <- as.list(envdata$value)
  names(ll) <- envdata$name
  ll$name <- "MonetDBConnection"
  ll$db.version <- NA
  ll$dbname <- ll$gdk_dbname
  ll$username <- NA
  ll$host <- NA
  ll$port <- NA
  ll
})

# DBIObject > dbIsValid()
#' @export
#' @rdname MonetDBConnection-class
setMethod("dbIsValid", "MonetDBConnection", function(dbObj, ...) {
  return(invisible(!is.na(tryCatch(
    {
      dbGetInfo(dbObj)
      TRUE
    },
    error = function(e) {
      NA
    }
  ))))
})

### dbSendUpdate() and friends ###
#' @title Send a data-altering SQL statement to the database.
#' @description
#' The function `dbSendUpdate()` is used to send a data-altering statement
#' to a MonetDB database, e.g. `CREATE TABLE` or `INSERT`. As a
#' convenience feature, a placeholder (`?` character) can be used in the
#' SQL statement, and bound to parameters given in the varargs group before
#' execution. This is especially useful when scripting database updates, since
#' the parameters will be automatically quoted.
#'
#' The `dbSendUpdateAsync()` function works in a similar way as
#' `dbSendUpdate()``, except that the former should be used
#' when the database update is called from finalisers, to avoid very esoteric
#' concurrency problems. The `dbSendUpdateAsync()` function can be used in
#' finalisers to not mess up the socket. Here, the update is not guaranteed.
#' @param conn
#'        A MonetDB.R database connection, created using [DBI::dbConnect()] with
#'        the [MonetDB.R()] database driver.
#' @param statement
#'        A SQL statement to be sent to the database, e.g. `UPDATE` or
#'        `INSERT`.
#' @param ...
#'        Parameters to be bound to '?' characters in the query, similar to
#'        JDBC.
#' @param list A list of extra parameters.
#' @param async
#'        Behaves like `dbSendUpdateAsync()` Defaults to `FALSE`.
#' @return TRUE update was successful
#' @seealso [DBI::dbSendQuery()]
#'
#' @examples
#' library(DBI)
#' # Only run the examples on systems with the default MonetDB connection:
#' if (foundDefaultMonetDBdatabase()) {
#'   conn <- dbConnect(MonetDB.R())
#'
#'   dbSendUpdate(conn, "CREATE TABLE foo(a INT,b VARCHAR(100))")
#'   dbSendUpdate(conn, "INSERT INTO foo VALUES(?,?)", 42, "bar")
#'
#'   dbSendUpdateAsync(conn, "INSERT INTO foo VALUES(?,?)", 24, "bar")
#'   dbSendUpdateAsync(conn, "DROP TABLE foo")
#'
#'   dbDisconnect(conn)
#' }
#' @export
#' @rdname dbSendUpdate
setGeneric(
  "dbSendUpdate",
  function(conn, statement, ..., async = FALSE) {
    standardGeneric("dbSendUpdate")
  }
)

#' @export
#' @rdname dbSendUpdate
setMethod(
  "dbSendUpdate",
  signature(conn = "MonetDBConnection", statement = "character"),
  function(conn, statement, ..., list = NULL, async = FALSE) {
    if (!is.null(list) || length(list(...))) {
      if (length(list(...))) {
        statement <- .bindParameters(statement, list(...))
      }
      if (!is.null(list)) {
        statement <- .bindParameters(statement, list)
      }
    }
    res <- dbSendQuery(conn, statement, async = async)
    if (!res@env$success) {
      stop(paste(statement, "failed!\nServer says:", res@env$message))
    }
    return(invisible(TRUE))
  }
)

#' @export
#' @rdname dbSendUpdate
setGeneric(
  "dbSendUpdateAsync",
  function(conn, statement, ...) standardGeneric("dbSendUpdateAsync")
)

#' @export
#' @rdname dbSendUpdate
setMethod(
  "dbSendUpdateAsync",
  signature(conn = "MonetDBConnection", statement = "character"),
  function(conn, statement, ..., list = NULL) {
    dbSendUpdate(conn, statement, ..., list, async = TRUE)
  }
)
