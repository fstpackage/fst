
# Checks before releasing to CRAN

* Build and test package on:
    - Clang 6.0.0 (on latest Ubuntu)
    - R-hub infrastructure (all available platforms)
    - docker with the rocker/r-devel-ubsan-clang instrumented image
    - docker with the rocker/r-devel-san instrumented image
    - Travis Linux and OSX
    - AppVeyor (Windows Server)
    - latest R dev version on Windows
* Test packages with dependencies on fst using revdepcheck::revdep_check()
* Merge develop branch into release branch
* Bump version to even value in DESCRIPTION and check package startup message
* Update README.Rmd and verify generated README.md on Github (release)
* Update cran_comments.md
* Build docs folder using pkgdown::build_site()
* Update NEWS.md and make sure to remove '(in development)' in the version title and update version number
* Credit all GitHub contributions in NEWS.md
* Submit to CRAN

# After releasing to CRAN

* Update NEWS.md with the release date
* Build docs folder using pkgdown::build_site(). Check that the package date is correct.
* Merge branch release into master
* Tag the release on Github
* Commit latest docs to the fstpackage.github.io and fstpackage.github.io/fst repository
* Go to the repository release page and create a new release with tag version vx.y.z.
  Copy and paste the contents of the relevant NEWS.md section into the release notes.
* Merge branch master into release
* Add '(in development)' to version title in NEWS.md and update to odd version number
* Check package startup message
* Merge release branch into develop
