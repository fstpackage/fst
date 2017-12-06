
## Test environments

* OS X on travis-ci (old, release and devel)
* Ubuntu 14.04 on travis-ci (old, release and devel)
* Ubuntu 16.04 locally
* docker with the rocker/r-devel-ubsan-clang instrumented image
* docker with the rocker/r-devel-san instrumented image
* Windows 10 local R 3.4.2
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor R 3.4.2
* R-hub all available platforms. Note: OpenMP is not enabled on most platforms, so limited testing possible.

## R CMD check results

There were no ERRORs or WARNINGs. 
There were no NOTEs

## Downstream dependencies

I have run R CMD check on downstream dependencies:

* heims: runs without warnings or errors.
* rio: runs without warnings or errors.
