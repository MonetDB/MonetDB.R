#' @include MonetDBConnection.R

#' @title Table read/write functions
#' @description
#' Functions to read/write data from/into MonetDB SQL tables.
#'
#' [dbListTables()] lists all user created database tables by default. It can
#' also be instructed to list all system tables and to list all tables with
#' their schema names.
#' @examples
#' library(DBI)
#' if (foundDefaultMonetDBdatabase()) {
#'   conn <- dbConnect(MonetDB.R())
#'   dbWriteTable(conn, "mtcars", mtcars[1:5, ])
#'   dbWriteTable(conn, "mtcars", mtcars[5:10, ], overwrite = TRUE)
#'   dbWriteTable(conn, "mtcars", mtcars[11:15, ], append = TRUE)
#'   dbWriteTable(conn, "mtcars", mtcars[11:15, ],
#'     append = TRUE, csvdump = TRUE
#'   )
#'   dbWriteTable(conn, "iris", iris, append = TRUE, temporary = TRUE)
#'   dbListTables(conn)
#'   dbListFields(conn, "mtcars")
#'   dbReadTable(conn, "iris")
#'   if (dbExistsTable(conn, "iris")) {
#'     dbRemoveTable(conn, "mtcars")
#'   }
#'   dbRemoveTable(conn, "iris")
#'   dbDisconnect(conn)
#' }
#' @param conn
#'   A MonetDB.R database connection, created using [DBI::dbConnect] with the
#'   [MonetDB.R] database driver.
#' @param sys_tables
#'   List the system tables? Default: F
#' @param schema_names
#'   Include the schema of the tables? Default: F
#' @export
#' @rdname tables
setMethod(
  "dbListTables", "MonetDBConnection",
  function(conn, ..., sys_tables = F, schema_names = F) {
    q <- paste(
      "SELECT schemas.name AS sn, tables.name AS tn",
      "FROM sys.tables JOIN sys.schemas",
      "ON tables.schema_id = schemas.id"
    )
    if (!sys_tables) {
      q <- paste0(q, " WHERE tables.system = FALSE ORDER BY sn, tn")
    }
    df <- DBI::dbGetQuery(conn, q)
    df$tn <- quoteIfNeeded(conn, df$tn)
    res <- df$tn
    if (schema_names) {
      df$sn <- quoteIfNeeded(conn, df$sn)
      res <- paste0(df$sn, ".", df$tn)
    }
    as.character(res)
  }
)

# dbListFields()
#' @param name The name of the database table.
#' @export
#' @rdname tables
setMethod(
  "dbListFields",
  signature(conn = "MonetDBConnection", name = "character"),
  function(conn, name, ...) {
    if (!dbExistsTable(conn, name)) {
      stop(paste0("Unknown table: ", name))
    }
    df <- DBI::dbGetQuery(conn, paste0(
      "SELECT columns.name AS name FROM sys.columns JOIN sys.tables ON ",
      "columns.table_id = tables.id WHERE tables.name = '", name, "';"
    ))
    df$name
  }
)

# dbReadTable()
#' @export
#' @rdname tables
setMethod(
  "dbReadTable",
  signature(conn = "MonetDBConnection", name = "character"),
  function(conn, name, ...) {
    name <- quoteIfNeeded(conn, name)
    if (!dbExistsTable(conn, name)) {
      stop(paste0("Unknown table: ", name))
    }
    DBI::dbGetQuery(conn, paste0("SELECT * FROM ", name), ...)
  }
)

# dbWriteTable()
#' @description
#' [dbWriteTable()] executes several SQL statements in a single transaction to
#' create/overwrite a database table and fill it with values, or append the
#' values to an existing database table. It returns a boolean to indicate
#' success or failure.
#'
#' @param name
#'        The name of the database table.
#' @param value
#'        The dataframe that needs to be stored in the table
#' @param overwrite
#'        Overwrite the whole table with dataframe. Default `FALSE`.
#' @param append
#'        Append dataframe to table
#' @param csvdump
#'        Dump dataframe to a temporary CSV file, and then import that CSV file.
#'        Can be used for performance reasons. Default `FALSE`.
#' @param transaction
#'        Wrap operation in transaction. Default: `TRUE`.
#' @param temporary
#'        Create a temporary table instead of a normal SQL table (i.e.
#'        persistent). Default: `FALSE`.
#' @param ... Any other parameters. Passed to [monetdb.read.csv()]
#' @export
#' @rdname tables
setMethod(
  "dbWriteTable",
  signature(conn = "MonetDBConnection", name = "character", value = "ANY"),
  function(conn, name, value, overwrite = FALSE, append = FALSE,
           csvdump = FALSE, transaction = TRUE, temporary = FALSE, ...) {
    if (is.character(value)) {
      message(paste(
        "Treating character vector parameter as file name(s)",
        "for monetdb.read.csv()"
      ))
      monetdb.read.csv(
        conn = conn, files = value, tablename = name,
        create = !append, ...
      )
      return(invisible(TRUE))
    }
    if (is.vector(value) && !is.list(value)) {
      value <- data.frame(x = value, stringsAsFactors = F)
    }
    if (length(value) < 1) stop("value must have at least one column")
    if (is.null(names(value))) {
      names(value) <- paste("V", 1:length(value), sep = "")
    }
    if (length(value[[1]]) > 0) {
      if (!is.data.frame(value)) {
        value <- as.data.frame(value,
                               row.names = 1:length(value[[1]]),
                               stringsAsFactors = F
        )
      }
    } else {
      if (!is.data.frame(value)) {
        value <- as.data.frame(value, stringsAsFactors = F)
      }
    }
    if (overwrite && append) {
      stop("Setting both overwrite and append to TRUE makes no sense.")
    }
    if (transaction) {
      dbBegin(conn)
      on.exit(tryCatch(dbRollback(conn), error = function(e) {}))
    }
    qname <- quoteIfNeeded(conn, name)
    if (dbExistsTable(conn, qname)) {
      if (overwrite) dbRemoveTable(conn, qname)
      if (!overwrite && !append) {
        stop(paste(
          "Table", qname, "already exists.",
          "Set overwrite = TRUE if you want to remove the existing table.",
          "Set append = TRUE if you would like to add the new data ",
          "to the existing table."
        ))
      }
    }

    if (!dbExistsTable(conn, qname)) {
      fts <- sapply(value, dbDataType, dbObj = conn)
      fdef <- paste(quoteIfNeeded(conn, names(value)), fts, collapse = ", ")
      if (temporary) {
        ct <- paste0(
          "CREATE TEMPORARY TABLE ", qname, " (", fdef,
          ") ON COMMIT PRESERVE ROWS"
        )
      } else {
        ct <- paste0("CREATE TABLE ", qname, " (", fdef, ")")
      }
      dbSendUpdate(conn, ct)
    }
    if (length(value[[1]])) {
      classes <- unlist(lapply(value, class))
      for (c in names(classes[classes == "character"])) {
        value[[c]] <- enc2utf8(value[[c]])
      }
      for (c in names(classes[classes == "factor"])) {
        levels(value[[c]]) <- enc2utf8(levels(value[[c]]))
      }
    }
    if (csvdump) {
      tmp <- tempfile(fileext = ".csv")
      write.table(value, tmp,
                  sep = ",", quote = TRUE, row.names = FALSE,
                  col.names = FALSE, na = "", fileEncoding = "UTF-8"
      )
      dbSendQuery(conn, paste0(
        "COPY INTO ", qname, " FROM '", tmp,
        "' USING DELIMITERS ',','\\n','\"' NULL AS ''"
      ))
      file.remove(tmp)
    }
    else {
      vins <- paste("(", paste(rep("?", length(value)), collapse = ", "), ")",
                    sep = ""
      )
      # chunk some inserts together so we do not need to do a round trip for
      # every one
      splitlen <- 0:(nrow(value) - 1) %/%
        getOption("monetdb.insert.splitsize", 1000)
      lapply(
        split(value, splitlen),
        function(valueck) {
          bvins <- c()
          for (j in 1:length(valueck[[1]])) {
            bvins <- c(bvins, .bindParameters(vins, as.list(valueck[j, ])))
          }
          dbSendUpdate(conn, paste0(
            "INSERT INTO ", qname, " VALUES ",
            paste0(bvins, collapse = ", ")
          ))
        }
      )
    }
    if (transaction) {
      dbCommit(conn)
      on.exit(NULL)
    }
    return(invisible(TRUE))
  }
)

# dbExistsTable()
#' @export
#' @rdname tables
setMethod(
  "dbExistsTable",
  signature(conn = "MonetDBConnection", name = "character"),
  function(conn, name, ...) {
    name <- quoteIfNeeded(conn, name)
    return(as.character(name) %in%
             dbListTables(conn, sys_tables = T))
  }
)

# dbRemoveTable()
#' @description
#' [dbRemoveTable()] removes the given database table. It returns TRUE to
#' indicate success removal or FALSE if the table does not exist.
#' @export
#' @rdname tables
setMethod(
  "dbRemoveTable",
  signature(conn = "MonetDBConnection", name = "character"),
  function(conn, name, ...) {
    name <- quoteIfNeeded(conn, name)
    if (dbExistsTable(conn, name)) {
      dbSendUpdate(conn, paste("DROP TABLE", name))
      return(invisible(TRUE))
    }
    return(invisible(FALSE))
  }
)
