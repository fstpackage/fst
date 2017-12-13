
# Checks before releasing to CRAN

* Start release branch from develop
* Bump version to even value in DESCRIPTION and check package startup message
* Update README.Rmd and verify generated README.md on Github (release)
* Update NEWS.md and make sure to remove '(in development)' in version title
* Credit all GitHub contributions in NEWS.md
* Submit to CRAN

# After releasing to CRAN

* Merge branch master into release
* Go to the repository release page and create a new release with tag version vx.y.z. Copy and paste the contents of the relevant NEWS.md section into the release notes.
* Add '(in development)' to version title in NEWS.md
* Bump version to odd value and check package startup message
* Merge release branch into develop
