
## Submission

This submission of fst (v0.9.4) addresses Prof. Brian Ripley's request to fix problems shown in updated CRAN checks. Packages that are in Suggests are now used conditionally in the package code and tests.

## Test environments 

* OSX on travis-ci (version 10.13.6)
* Ubuntu Ubuntu 16.04.6 LTS on travis-ci
* Ubuntu 20.04 locally using clang-10.0
* Docker with the rocker/r-devel-ubsan-clang instrumented image
* Local Ubuntu with instrumented image using clang-10
* Windows 10 local R 3.6.4
* Windows 10 local R-dev 4.0
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor (R 3.6.3)
* Singularity-container package for running rchk on Ubuntu 18.10
* Valgrind on Ubuntu 20.04
* Rhub (all available systems)

## R CMD check results

There are no errors or warnings.

## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues. The notes on the grattan and tidyfst packages are unrelated to fst.

-- CHECKED 9 packages --

disk.frame 0.3.4
drake 7.11.0
expss 0.10.1
grattan 1.8.0.0 ->  1 note unrelated to fst
hdd 0.1.0
heims 0.4.0
readabs 0.4.3
rio 0.5.16
tidyfst 0.7.7 ->  1 note unrelated to fst
