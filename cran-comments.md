
## Resubmission

This resubmission of fst (v0.9.4) addresses the submission comments of Dr. Uwe Ligges on the package title and URL.

## Submission

This submission of fst (v0.9.4) addresses Prof. Brian Ripley's request to fix problems shown in updated CRAN checks. Packages that are in Suggests are now used conditionally in the package code and tests.

## Test environments 

* OSX on travis-ci (version 10.13.6)
* Ubuntu 16.04.6 LTS on travis-ci
* Ubuntu 18.04 locally using clang-10.0
* Docker with the rocker/r-devel-ubsan-clang instrumented image
* Local Ubuntu with instrumented image using clang-10
* Windows 10 local R 3.6.4
* Windows 10 local R-dev 4.0.2
* Singularity-container package for running rchk on Ubuntu 18.04
* Valgrind on Ubuntu 18.04
* Rhub (all available systems)

## R CMD check results

There are no errors or warnings.

## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues. The notes on the grattan and tidyfst packages are unrelated to fst.

-- CHECKED 15 packages --

disk.frame  0.3.7
drake       7.12.4
epwshiftr   0.1.1
expss       0.10.6
grattan     1.9.0.0 ->  1 note unrelated to fst
hdd         0.1.0
heims       0.4.0
lazyarray   1.1.0
prt         0.1.0
qtl2fst     0.22-7
raveio      0.0.3
readabs     0.4.3
rio         0.5.16
tidyfst     0.9.8 ->  1 note unrelated to fst
tidyft      0.4.5 ->  1 note unrelated to fst
