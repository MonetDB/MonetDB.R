# R connector for MonetDB.

## Installation
Installation can be done through source:

```r
install.packages('/path/to/tar.gz', repos=NULL, type='source')
```
The DBI library is used for basic database manipulation.

## Usage
First we need to define a connection:
```r
conn = DBI::dbConnect(MonetDB.R::MonetDB(), '<database-name>')
```

If the `<database-name>` argument is omitted, it will default to the 'demo' database.

## Unit testing
The unit tests can be run from the shell:

```r
library(testthat)
testthat::test_dir('tests')
```

Note: There could be an issue with the csv imports. The daemon has insufficient rights to
read the generated csv file. To fix this, run mserver5 yourself.

Note: The tests will not run if the NOT_CRAN system variable is not set to "true".


## Man pages
This packages provides man pages with explanation and examples of various functions.
They can be accessed from the shell:

```r
help('MonetDB.R')
# or:
?MonetDB.R
```


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
	R/dbapply.R
	R/dbi.R
	R/dplyr.R
	R/mapi.R
	man/MonetDB.R.Rd
	man/control.Rd
	man/dbApply.Rd
	man/dbSendUpdate.Rd
	man/monetdb.read.csv.Rd
	man/monetdbRtype.Rd
	man/monetdbd.liststatus.Rd
	man/sqlitecompat.Rd
	man/src_monetdb.Rd
	src/embeddedr/mapisplit-r.c
	src/embeddedr/mapisplit.c
	src/embeddedr/mapisplit.h



