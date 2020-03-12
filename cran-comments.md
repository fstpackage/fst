
## Submission

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
* Rhub (only on systems that support OpenMP)

## R CMD check results

There were no ERRORs or WARNINGs.

## Downstream dependencies

I have run R CMD check on downstream dependencies and found no issues. The notes on the grattan
and tidyfst packages are unrelated to fst.

-- CHECK --------------------------------------------------------- 9 packages --
disk.frame 0.3.4                       -- E: 0     | W: 0     | N: 0
drake 7.11.0                           -- E: 0     | W: 0     | N: 0
expss 0.10.1                           -- E: 0     | W: 0     | N: 0
grattan 1.8.0.0                        -- E: 0     | W: 0     | N: 1
hdd 0.1.0                              -- E: 0     | W: 0     | N: 0
heims 0.4.0                            -- E: 0     | W: 0     | N: 0
readabs 0.4.3                          -- E: 0     | W: 0     | N: 0
rio 0.5.16                             -- E: 0     | W: 0     | N: 0
tidyfst 0.7.7                          -- E: 0     | W: 0     | N: 1
