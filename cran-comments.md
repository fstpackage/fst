
## Submission

In this submission of fst, the advance warning from Prof. Ripley is addressed where fst fails to install with the Clang 6.0.0 trunk (caused by the 'deprecated' keyword). The issue has been addressed at the LZ4 source repository and a new version of LZ4 has been released in which the problem is addressed. That version is now included in fst.

The CRAN valgrind build results contain multiple occurences of the following warnings:
    - 'Syscall param write(buf) points to uninitialised byte(s)'
    - 'Conditional jump or move depends on uninitialised value(s)'
These messages are due to writing uninitialised header blocks to file. This is done intentionally (for speed) and the header is later overwritten with correct values.

The CRAN r-patched-solaris-x86 build shows errors in the unit tests. These errors are due to the endianness of the Solaris platform and the main reason why 'SystemRequirements: little-endian platform' is added to the DESCRIPTION file.

## Test environments

* OS X on travis-ci
* Ubuntu 14.04 on travis-ci
* Ubuntu 16.04 locally
* docker with the rocker/r-devel-ubsan-clang instrumented image
* docker with the rocker/r-devel-san instrumented image
* Windows 10 local R 3.4.3
* Windows Server 2012 R2 x64 (build 9600) on AppVeyor R 3.4.3
* R-hub all available platforms. Note: OpenMP is not enabled on most platforms, so limited testing possible.

## R CMD check results

There were no ERRORs or WARNINGs.

On CRAN r-devel-linux-x86_64-fedora-gcc there is a note:
   'installed size is 6.8Mb'.
The install size on different platforms varies significantly; from 1.42 MB (windows 10) to 6.8 MB on fedora (only the fedora build reports a size larger than 5 MB).

## Downstream dependencies

I have run R CMD check on downstream dependencies:

* heims: runs without warnings or errors.
* rio: runs without warnings or errors.
