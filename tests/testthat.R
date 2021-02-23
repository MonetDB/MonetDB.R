library(testthat)
library(MonetDB.R)

run <- function() {
  return(invisible(
    identical(Sys.getenv("NOT_CRAN"), "true") &&
      foundDefaultMonetDBdatabase()
  ))
}

if (run()) {
  test_check("MonetDB.R")
}
