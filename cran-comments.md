
## Submission

With this release of fst (v0.9.8), the fstlib library is no longer build with the fst package, but
imported from package `fstcore`. This allows for better separation of updates to the fstlib C++ library and the fst
wrapper package and avoids duplicate code in fst and fstcore. Packages can also directly use the interface
exported from the fstcore package from C/C++ code.

With this new setup the linker problem on macOS systems is also resolved.

## Test environments 

* macOS 11.6.2 20G314 using R 4.1.2 on github build infrastructure
* Ubuntu 20.04.3 LTS using R version 4.0.5 on github build infrastructure
* Ubuntu 20.04.3 LTS using R 4.1.2 on github build infrastructure
* Ubuntu 20.04.3 LTS using R dev (2022-01-30 r81596) on github build infrastructure
* Microsoft Windows Server 2019 10.0.17763 Datacenter using R 4.1.2 on github build infrastructure
* Ubuntu 18.04 locally using clang-10.0
* Docker with the rocker/r-devel-ubsan-clang instrumented image
* Local Ubuntu with instrumented image using clang-10
* Windows 11 local R 3.6.3
* Windows 11 local R 4.1.2
* Singularity-container package for running rchk on Ubuntu 18.04
* Valgrind on Ubuntu 18.04
* Rhub (all available systems)

## R CMD check results

There are no errors or warnings.

## revdepcheck results

We checked 29 reverse dependencies (26 from CRAN + 3 from Bioconductor), comparing R CMD check results across CRAN and dev versions of this package.

 * We saw 0 new problems
 * We failed to check 0 packages
