
## Submission

In this submission of fst, build errors reported in the CRAN check results for version 0.8.4 are addressed (thanks Kurt Hornik for the warning). These errors were due to failing unit tests and have been traced back to changes in the data.table code base (for the ITime type) between version 1.10.4-3 and 1.11.0. All issues have been resolved in this release.

## Test environments

* OS X on travis-ci
* Ubuntu 14.04 on travis-ci
* Ubuntu 16.04 locally
* Ubuntu 17.04 locally using clang-6.0
* docker with the rocker/r-devel-ubsan-clang instrumented image
* docker with the rocker/r-devel-san instrumented image
* Windows 10 local R 3.5.0
* Windows 10 local R-dev
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor R 3.5.0

## R CMD check results

There were no ERRORs or WARNINGs.

On some platforms a note is generated with R CMD check:
   'installed size is 7.0Mb'.
The install size on different platforms varies significantly, from 1.42 MB (windows 10) to more than 7 MB on fedora.

## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues:

* heims: runs without warnings or errors.
* rio: runs without warnings or errors.
