\name{dbWriteTable}
\alias{dbWriteTable}

\title{
  Write, append or overwrite a data frame to a database table
}
\description{
\code{dbWriteTable} is used to write, overwrite or append a data frame to a database table
  
}
\usage{
  dbWriteTable(conn, name, value, overwrite=F, append=F, csvdump=F, transaction=T, temporary=F ...)
}
\arguments{
  \item{conn}{A MonetDB.R database connection. Created using \code{\link[DBI]{dbConnect}} 
   with the \code{\link[MonetDB.R]{MonetDB.R}} database driver.}

  \item{name}{The name of the database table. }

  \item{value}{The dataframe that needs to be stored in the table}

  \item{overwrite}{Overwrite whole table with dataframe. default: \code{False}}
   
  \item{append}{Append dataframe to table. Default: \code{False}}

  \item{csvdump}{Dump dataframe to a temporary CSV file, and then import that CSV file. Can be used for performance reasons. Default \code{False}}

  \item{transaction}{Wrap operation in transaction. Default: \code{True}}

  \item{temporary}{Create a temporary table instead of a 'real' table Default: \code{False}}

  \item{...}{Future use.}

 }
\value{
  Returns \code{TRUE} if the writetable command was successful.
}
\examples{
\dontrun{
conn <- dbConnect(MonetDB.R(), "monetdb://localhost/acs")

# Write a table with the name "mtcars" to the database.
dbWriteTable(conn, "mtcars", mtcars[1:5,])

# Overwrite the table with different data.
dbWriteTable(conn, "mtcars", mtcars[5:10,], overwrite=T)

# Append to a table
dbWriteTable(conn, "mtcars", mtcars[11:15,], append=T)

# Append, and use csvdump
dbWriteTable(conn, "mtcars", mtcars[11:15,], append=T, csvdump=T)

# Create a temporary table.
dbWriteTable(conn, "iris", iris, temporary=T)
}}
