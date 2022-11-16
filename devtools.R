## DESCRIPTION
use_packages <- list(
  Imports = c(
    renv::dependencies("R/")$Package
  ),
  Depends = c(),
  Suggests = c(),
  Enhances = c(),
  LinkingTo = c()
)

purrr::walk2(names(use_packages), use_packages, function(type, packages) {
  purrr::walk(packages, usethis::use_package, type = type, min_version = TRUE)
})


## Testing
usethis::use_testthat()


## Documentation
usethis::use_package_doc()


## NAMESPACE, collation, etc.
devtools::document()

