
## Resubmission

This is a resubmission. In this version I have:

* Added the authors of LZ4 and ZSTD and their copyright holders to the authors field in file DESCRIPTION.

* Changed the Description field in file DESCRIPTION to comply with CRAN policy and add clear and unambiguous
the ownership of copyright and intellectual property rights of all components of the package.

* The package now meets the 'Redistributions in binary form' condition of the licence for ZSTD and LZ4. A copy
of both licenses is now included in the package documentation. Man file 'fst-package.Rd' now lists both licenses.

* Update the version of xxHash to a newer version. The new version does not use the restrict keyword in the
C++ sources (which causes the  gcc 5.2 build on Solaris to fail)

* The CRAN valgrind build results contain the warning 'Subdirectory 'fst/src' contains apparent object files/libraries'. With this resubmission I have made sure no object files are present in the library before calling devtools::release()

* The CRAN valgrind build results contain multiple occurences of the following warnings:
    - 'Syscall param write(buf) points to uninitialised byte(s)'
    - 'Conditional jump or move depends on uninitialised value(s)'
  These messages are due to writing uninitialised header blocks to file. This is done intentionally (for speed) and the header is later overwritten with correct values.
    
* The CRAN valgrind build contains in the HEAP summary:
    - '24 bytes in 1 blocks are definitely lost in loss record 17 of 1,764'
    The source of this leak has been located and fixed (missing delete).

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

On r-devel-linux-x86_64-fedora-gcc there is a note:
   'installed size is 6.8Mb'.
   The install size on different platforms vary significantly; from 1.42 MB (windows 10) to 6.8 MB on fedora and only on fedora is the size larger than 5 MB.


## Downstream dependencies

I have run R CMD check on downstream dependencies:

* heims: runs without warnings or errors.
* rio: runs without warnings or errors.
