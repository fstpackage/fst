
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

On some platforms, the following warning is generated for the included ZSTD compressor component (from Facebook):

"warning: ISO C99 requires rest arguments to be used"

on files:

* zstd_decompress.c
* zstd_decompress_block.c
* zstd_decompress.c
* zstd_ddict.c
* zstd_compress_sequences
* zstd_compress_literals.c
* zstd_cwksp.h

The warnings are generated when compiler option '-pedantic' is used and are all due to macro's defined with
ellipses that are left empty when called. I have raised an issue with the ZSTD team addressing this problem
(see https://github.com/facebook/zstd/issues/2035).

Currently, this warning makes fst fail the winbuilder pre-tests. However, because the ZSTD library is thoroughly
tested on many platforms and configurations, locally patching the code responsible for the issue is probably less
save than accepting these warnings until fixed. Please let me know if you feel differently.

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
