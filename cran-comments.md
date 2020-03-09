
## Submission

This submission of fst (v0.9.2) addresses Dr. Kurt Hornik's request to fix problems shown for the r-devel checks. These issues are related to the new stringsAsFactors = FALSE default, which is planned for R 4.0.0.

## Test environments 

* OSX on travis-ci
* Ubuntu 14.04 on travis-ci
* Ubuntu 18.10 locally
* Ubuntu 18.10 locally using clang-6.0
* Docker with the rocker/r-devel-ubsan-clang instrumented image
* Docker with the rocker/r-devel-san instrumented image
* Windows 10 local R 3.5.3
* Windows 10 local R-dev 3.6.0 pre-release
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor (R 3.5.3)
* Singularity-container package for running rchk on Ubuntu 18.10
* Valgrind on Ubuntu 18.10.
* Rhub (only on systems that support OpenMP)

## R CMD check results

There were no ERRORs or WARNINGs.

## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues:

* heims: runs without warnings or errors.
* rio: runs without warnings or errors.
* grattan: runs without warnings or errors.
