library(DBI)

test_that("all aliases work", {
  expect_equal(MonetDB.R::dbIsValid(MonetDB.R()), TRUE)
  expect_equal(MonetDB.R::dbIsValid(MonetR()), TRUE)
  expect_equal(MonetDB.R::dbIsValid(MonetDB()), TRUE)
  expect_equal(MonetDB.R::dbIsValid(MonetDBR()), TRUE)

  expect_that(MonetDB.R(), is_a("MonetDBDriver"))
  expect_that(MonetR(), is_a("MonetDBDriver"))
  expect_that(MonetDB(), is_a("MonetDBDriver"))
  expect_that(MonetDBR(), is_a("MonetDBDriver"))
})
