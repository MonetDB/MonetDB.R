library(testthat)

not_on_cran <- function() {
    return(invisible(identical(Sys.getenv("NOT_CRAN"), "true")))
}


if(not_on_cran()) {
    test_check("MonetDB.R")
}