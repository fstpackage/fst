
## Resubmission

This submission of fst (v0.9.2) addresses Dr. Kurt Hornik's request to fix problems shown for the r-devel checks. These issues are related to the new stringsAsFactors = FALSE default, which is planned for R 4.0.0.

## Test environments 

* OSX on travis-ci (version 10.13.6)
* Ubuntu Ubuntu 16.04.6 LTS on travis-ci
* Ubuntu 19.10 locally
* Ubuntu 19.10 locally using clang-6.0
* Docker with the rocker/r-devel-ubsan-clang instrumented image
* Docker with the rocker/r-devel-san instrumented image
* Windows 10 local R 3.6.3
* Windows 10 local R-dev 4.0.0 pre-release (r77640)
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor (R 3.6.3)
* Singularity-container package for running rchk on Ubuntu 18.10
* Valgrind on Ubuntu 19.10.
* Rhub (all available systems)

## R CMD check results

The pre-checks for my previous submission failed on a specific warning in the included ZSTD compressor component (from Facebook). To remedy this, I've adapted the C++ macro responsible for the warning and all corresponding calls. This could be safely done as the macro is only activated in a specially prepared DEBUG build and therefore it is not used bycalls from the fst package.

I have raised an issue with the ZSTD team addressing this problem
(see https://github.com/facebook/zstd/issues/2035).


## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues. The notes on the grattan and tidyfst packages are unrelated to fst.

-- CHECKED 9 packages --

disk.frame 0.3.4
drake 7.11.0
expss 0.10.1
grattan 1.8.0.0   ->  1 note unrelated to fst
hdd 0.1.0
heims 0.4.0
readabs 0.4.3
rio 0.5.16
tidyfst 0.7.7   ->  1 note unrelated to fst
