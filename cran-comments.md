
## Submission

This submission of fst (v0.9.0) addresses Dr. Kurt Hornik's request to fix issues identified by rchk. These issues result from PROTECT / UNPROTECT pairs called in the constructor / destructor pairs of C++ classes. rchk (rightfully) warns about those because it can't determine from the code if pairs are properly matched. With this submission the relevant SEXP classes are protected by containing them in SEXP classes that are already PROTECTED, which allows for removal of the PROTECT / UNPROTECT pairs in question.

Two false warnings remain, detected in fst_compress.cpp lines 134 and 164. The code was thoroughly checked to affirm the stability of the code.

In addition, support for fst files generated with package versions before 0.8.0 has been deprecated, significantly reducing the (C++) code base.

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
