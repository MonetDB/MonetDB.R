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

## Man pages
This packages provides man pages with explanation and examples of various functions.
They can be accessed from the shell:

```r
help('MonetDB.R')
# or:
?MonetDB.R
```

