# A DBI-compliant R interface to MonetDB

MonetDB.R is an [DBI](https://cran.r-project.org/web/packages/DBI/index.html)-compliant interface to the open-source [MonetDB](https://www.monetdb.org) database.  It also comes with several MonetDB specific functions (see "MonetDB extensions" below).

NB: MonetDB.R version >=2.0.0 only works with MonetDB Jun2020 release or newer. For older MonetDB versions, please use MonetDB.R version 1.*.

## Installation
```r
# Install the latest MonetDB.R release from CRAN:
install.packages('MonetDB.R')

#or from a source package:
install.packages('/path/to/MonetDB.R.tar.gz', repos=NULL, type='source')
```

## Usage

### Basic usage

Assuming MonetDB is running on localhost and a database named 'demo' exists and
is accessible with the default username/password combination: monetdb/monetdb.

```r
library(DBI)
# Connect to the default MonetDB database:
con <- DBI::dbConnect(MonetDB.R::MonetDB())

# Let's add a table:
dbWriteTable(con, "iris", iris)

# Have a look at the newly added table
dbListTables(con)
dbListFields(con, "iris")
dbReadTable(con, "iris")

# Fetch all results of a query:
res <- dbSendQuery(con, "SELECT * FROM iris WHERE \"Species\" = 'versicolor'")
dbFetch(res)
dbClearResult(res)

# Fetch N rows of results at a time
res <- dbSendQuery(con, "SELECT * FROM iris WHERE \"Species\" = 'versicolor'")
while(!dbHasCompleted(res)){
  chunk <- dbFetch(res, n=3)
  print(chunk)
  print(nrow(chunk))
}
dbClearResult(res)

# Disconnect from the database
dbDisconnect(con)
```
### Connect to a specific MonetDB instance:
```r
library(DBI)
# Connect to a specific MonetDB database:
con <- dbConnect(MonetDB.R::MonetDB(),
                 dbname = 'DATABASE_NAME', # e.g. 'demo'
                 host = 'HOSTNAME', # i.e. 'www.example.org'
                 port = 50000, # or any other port specified by your DBA
                 user = 'USERNAME',
                 password = 'PASSWORD')
```
### Man pages
This packages provides man pages with explanation and examples of various functions.
They can be accessed from the shell:

```r
help('MonetDB.R')
# or:
?MonetDB.R
```
### MonetDB extensions
This package also provides several MonetDB specitic functions to, e.g. control multistatements SQL transactions and bulk load CSV data using MonetDB's ``COPY INTO`` feature.  For their usage, please consume their man pages:
```r
# Use MonetDB's COPY INTO feature to bulk load CSV data into a database
?monetdb.read.csv

# Switch from the auto-commit mode to the transactional mode
?dbTransaction

# Send database altering statement to a MonetDB database
?dbSendUpdate
```

## Unit testing
The unit tests can be run from the shell:

```r
install.packages('devtools')
library(devtools)

# Run an extensive check including the unit tests:
devtools::check()
# Only run the unit tests:
testthat::test_dir('tests/testthat')
```
Note: The tests will not run if the NOT_CRAN system variable is not set to "true", which is done by ``tests/testthat.R``.

## Acknowledgements
This Source Code Form is subject to the terms of the Mozilla Public License, v.
2.0.  If a copy of the MPL was not distributed with this file, You can obtain
one at http://mozilla.org/MPL/2.0/.

Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.

Original author: Hannes Muehleisen
Additional authors: Mitchell Weggemans (since July 2020)

The following files originate from Hannes' repository
https://github.com/MonetDB/MonetDBLite-R (copied on July 17, 2020):

	DESCRIPTION
	NAMESPACE
	NEWS
	README.md
	R/dbi.R
	R/mapi.R
	man/MonetDB.R.Rd
	man/dbSendUpdate.Rd
	man/monetdb.read.csv.Rd
	src/embeddedr/mapisplit-r.c
	src/embeddedr/mapisplit.c
	src/embeddedr/mapisplit.h



