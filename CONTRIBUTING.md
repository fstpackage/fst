
Contributions to `fst` are welcome from anyone and are best sent as pull requests on [the GitHub repository](https://github.com/fstpackage/fst/). This page provides some instructions to potential contributors about how to add to the package.

 1. Contributions can be submitted as [a pull request](https://help.github.com/articles/creating-a-pull-request/) on GitHub by forking or cloning the [fst repository](https://github.com/fstpackage/fst/), making changes and submitting the pull request.
 
 2. Pull requests should involve only one commit per substantive change. This means if you change multiple files (e.g., code and documentation), these changes should be committed together. If you don't know how to do this (e.g., you are making changes in the GitHub web interface) just submit anyway and the maintainer will clean things up.

 3. Pull requests that involve code in the 3rd party libraries `LZ4`, `ZSTD` or `fstlib` (located under `src/fstcore`) will not be accepted. These libraries are used _as-is_ and any issues can be reported at their respective GitHub repositories.
 
 4. All contributions must be submitted consistent with the package license ([AGPL-3](https://www.gnu.org/licenses/agpl-3.0.en.html)).
 
 5. All contributions need to be noted in the `Authors@R` field in the [`DESCRIPTION`](https://github.com/fstpackage/fst/blob/master/DESCRIPTION) file. Just follow the format of the existing entries to add your name (and, optionally, email address). Substantial contributions should also be noted in [`inst/CITATION`](https://github.com/fstpackage/fst/blob/master/inst/CITATION).
 
 6. This package uses `royxgen` code and documentation markup, so changes should be made to `roxygen` comments in the source code `.R` files. If changes are made, `roxygen` needs to be run. The easiest way to do this is a command line call to: `Rscript -e devtools::document()`. Please resolve any `roxygen` errors before submitting a pull request.
 
 7. Please run `R CMD BUILD fst` and `R CMD CHECK fst_VERSION.tar.gz` before submitting the pull request to check for any errors.
 
 8. Changes requiring a new package dependency should be discussed on the GitHub issues page before submitting a pull request.
 
Any questions you have can be opened as GitHub issues or directed to markklik (at) gmail.com.
