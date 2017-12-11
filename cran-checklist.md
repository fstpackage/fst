
# Checks before releasing to CRAN

* Start release branch from develop
* Bump version to even value in DESCRIPTION and check package startup message
* Update README.Rmd and verify generated README.md on Github (release)
* Update NEWS.md and make sure to remove '(in development)' in version title
* Credit all GitHub contributions in NEWS.md
* Submit to CRAN

# After releasing to CRAN

* Merge branch master into release
* Update NEWS.md and add '(in development)' to version title
* Bump version to odd value and check package startup message
* Merge release branch into develop
