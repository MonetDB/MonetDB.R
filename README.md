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
conn = dbConnect(MonetDB.R::MonetDB(), '<database-name>')
```

If the '<database>' argument is left blank, it will default to the 'demo' database.

## Unit testing
The unit tests can be run from the shell:

```r
library(testthat)
testthat::test_dir('tests')
```

## Man pages
This packages provides man pages with explanation and examples of various functions.
They can be accessed from the shell:

```r
help('MonetDB.R')
# or:
?MonetDB.R
```

