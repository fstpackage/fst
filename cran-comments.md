
## Submission

This submission of fst adresses Prof. Ripley's request to move the OpenMP flag to PKG_CXXFLAGS.
In addition several minor issues have been resolved to increase package stability and the
libraries on which fst depends (fstlib, LZ4 and ZSTD) are updated to their latest version.

## Test environments

* OSX on travis-ci
* Ubuntu 14.04 on travis-ci
* Ubuntu 18.10 locally
* Ubuntu 18.10 locally using clang-6.0
* docker with the rocker/r-devel-ubsan-clang instrumented image
* docker with the rocker/r-devel-san instrumented image
* Windows 10 local R 3.5.1
* Windows 10 local R-dev 3.6.0 pre-release
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor R 3.5.1
* R-Devel 3.6.0 build on Windows 10

## R CMD check results

There were no ERRORs or WARNINGs.

On some platforms a note is generated with R CMD check:
   installed size is 7.0Mb
The install size on different platforms varies significantly, from 1.42 MB (windows 10) to more than 7 MB on fedora.

## Valgrind

To reproduce the CRAN valgrind report, an instrumented (level 2) build of R was constructed on a fresh Ubuntu 16.04 image using config.site and configure parameters as specified in the memtests README file on CRAN. That build shows no valgrind warnings using the current submision.

## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues:

* heims: runs without warnings or errors.
* rio: runs without warnings or errors.
* grattan: runs without warnings or errors.
