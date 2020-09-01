\name{dbSendQuery}
\alias{dbSendQuery}

\title{
  Execute a SQL statement on the database.
}

\description{
  \code{dbSendQuery} method only submits and synchronously executes
     the SQL query to the database engine.  It does _not_ extract any
     records - for that you need to use the ‘dbFetch()’ method, and
     then you must call ‘dbClearResult()’ when you finish fetching the
     records you need. For interactive use, you should almost always
     prefer \code{\link[DBI]{dbGetQuery}}
}
\usage{
  dbSendQuery( conn, statement, ..., list=NULL, async=FALSE )
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} 
   with the \code{\link[MonetDB.R]{MonetDB.R}} database driver.}
   \item{statement}{A SQL statement to be sent to the database, e.g. 'START TRANSACTION' or 'COMMIT'.}
   \item{...}{Parameters to be bound to '?' characters in the query, similar to JDBC.}
   \item{list}{Parameters to be bound to '?' characters in the query as a list, similar to JDBC.}
   \item{async}{Behave like \code{dbSendUpdateAsync}? Defaults to \code{FALSE}.}
}

\value{
  Returns \code{MonetDBResult} if the query was successful.
}
\seealso{
  \code{\link[DBI]{dbGetQuery}}
}
\examples{
\dontrun{
# connect to MonetDB
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")

# start a transaction
dbSendQuery(conn, 'START TRANSACTION')

# commit the transaction
dbSendQuery(conn, 'COMMIT')
}}
\keyword{interface}
