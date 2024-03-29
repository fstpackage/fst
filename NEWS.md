
# fst 0.9.9 (in development)

## Enhancements

## Bugs solved

## Library updates


# fst 0.9.8

With this release of `fst`, the `fstlib` library is no longer build with the `fst` package, but
imported from package `fstcore`. This allows for better separation of updates of the C++ code underlying the `fst`
format and the R wrapper package. Also, with this setup, other packages can directly use the C++ interface exported
from the fstcore package.

## Bugs solved

* The package had linking problems on ARM macOS systems, preventing a correct build. These problems originated from
minor differences between the xxHash implementations in the LZ4 and ZSTD libraries and incorrect linker parameters.

## Library updates

* Library LZ4 updated to version 1.9.3
* library ZSTD updated to version 1.5.2


# fst 0.9.6

Version 0.9.6 is a minor update to 'hotfix' an issue with the use of `sample.int()`. All calls to `sample.int()` now
explicitly reference a length one size object.

# fst 0.9.4

Version 0.9.4 is a minor update to 'hotfix' an issue with suggested packages as reported by CRAN.

## Bugs solved

* Packages `nanotime`, `bit64` and `lintr` are now used conditionally as is required for packages suggested in DESCRIPTION, see 'Writing R Extensions' 1.1.3.1 (thanks @CRAN for reporting)


# fst 0.9.2

Version 0.9.2 of the `fst` package brings support for zero-row table serialization and compression for long vectors. In addition, `fst` was prepared for the change in the default settings for the stringsAsFactors argument (data.frame) in R 4.0.0.

## Library updates

* Library `fstlib` updated to version 0.1.6
* Library LZ4 updated to version 1.9.2
* library ZSTD updated to version 1.4.4

## Enhancements

* Incorrect column selection gets a more informative error message (issue #138, thanks Jean-Luc and @Moohan for reporting).
* Long raw vectors can be hashed with hash_fst (issue #202)
* Empty tables are serialized correctly using `write_fst()` (issue #99)

## Bugs solved

* Ellipsis is forwarded in method `str.fst_table()` (issue #219, thanks @nbenn for reporting).
* Coloring is turned off for terminals that don't support it or when package `crayon` is not installed (issue #198, thanks @muschellij2 for reporting and the code fix).
* Method `metadata_fst()` correctly displays the key of the data table if column names are not in alphabetical order (issue #199, thanks @renkun-ken for the pull request).
* stringsAsFactors argument defaults to FALSE for upcoming R 4.0.0 (issue #234, thanks @CRAN for reporting)


# fst 0.9.0

Version 0.9.0 of the `fst` package addresses the request from CRAN maintainers to fix issues identified by rchk. These issues result from PROTECT / UNPROTECT pairs called in the constructor / destructor pairs of C++ classes. rchk (rightfully) warns about those because it can't determine from the code if pairs are properly matched. With this submission the relevant SEXP classes are protected by containing them in SEXP classes that are already PROTECTED, which allows for removal of the PROTECT / UNPROTECT pairs in question.

As of `fst` version 0.9.0, support for fst files generated with `fst` package versions lower than 0.8.0 has been deprecated. This significantly reduces the (C++) code base and prepares `fst` for future code changes.

## Library updates

* Library `fstlib` updated to version 0.1.1

## Enhancements

* Method `setnrofthreads` returns invisible result to avoid printing unwanted output (thanks @renkun-ken for the pull request)

## Bugs solved

* Empty subsets can be selected using `fst::fst` (thanks @renkun-ken for reporting)

## Documentation

Various documentation issues have been fixed (thanks @ginberg and @renkun-ken for the pull requests).


# fst 0.8.10

Version 0.8.10 of the `fst` package is an intermediate release designed to update the incorporated C++ libraries
to their latest versions and to fix reported issues. Also, per request of CRAN maintainers, the OpenMP build option was moved to the correct flag in the Makevars file, resolving a warning in the package check.

## Library updates

* Library `fstlib` updated to version 0.1.0

* Library `ZSTD` updated to version 1.3.7

* Library `LZ4` updated to version 1.8.3

## Bugs solved

* Method `compress_fst()` can handle vectors with sizes larger than 4 GB (issue #176, thanks @bwlewis for reporting)

* A _fst_ file is correctly read from a subfolder on a network drive where the user does not have access to the top-level folder (issues #136 and #175, thanks @xiaodaigh for reporting).

* The suggested data.table dependency is now properly escaped (issue #181, thanks @jangorecki for the pull request)

## Documentation

* Documentation updates (issue #158, thanks @HughParsonage for submitting)


# fst 0.8.8

Version 0.8.8 of the `fst` package is an intermediate release designed to fix valgrind warnings reported on CRAN builds (per request of CRAN maintainers). These warnings were due to `fst` writing uninitialized data buffers to file, which was done to maximize speed. To fix these warnings (and for safety), all memory blocks are now initialized to zero before being written to disk.


# fst 0.8.6

Version 0.8.6 of the `fst` package brings clearer printing of `fst_table` objects. It also includes optimizations for controlling the number of threads used by the package during reads and writes and after a fork has ended. The `LZ4` and `ZSTD` compression libraries are updated to their latest (and fastest) releases. UTF-8 encoded column names are now correctly stored in the `fst` format.

## New features

* More advanced printing generic of the `fst_table` reference object, showing column types, (possible) keys, and the table header and footer data (issue #131, thanks @renkun-ken for reporting and discussions).

* User has more control over the number of threads used by fst. Option 'fst_threads' can now be used to initialize the number of threads when the package is first loaded (issue #132, thanks to @karldw for the pull request).

* Option 'fst_restore_after_fork' can be used to select the threading behavior after a fork has ended. Like the `data.table` package, `fst` switches back to a single thread when a fork is detected (using OpenMP in a fork can lead to problems). Unlike `data.table`, the `fst` package restores the number of threads to it's previous setting when the fork ends. If this leads to unexpected problems, the user can set the 'fst_restore_after_fork' option to FALSE to disable that.
    
## Bugs solved

* Character encoding of column names correctly stored in the `fst` format (issue #144, thanks @shrektan for reporting and discussions).

## Documentation

* Improved accuracy of fst_table documentation regarding random row access (issue #143, thanks @martinblostein for pointed out the unclarity)

* Improved documentation on background threads during `write_fst()` and `read_fst()` (issue #121, thanks @krlmlr for suggestions and discussion)


# fst 0.8.4

The v0.8.4 release brings a `data.frame` interface to the `fst` package. Column and row selection can now be done directly from the `[` operator. In addition, it fixes some issues and prepares the package for the next build toolchain of CRAN.

## New features

* A `data.frame` interface was added to the package. The user can create a reference object to a `fst` file with method `fst`. That reference can be used like a `data.frame` and will automatically make column- and row- selections in the referenced `fst` file.

## Bugs solved

* Build issues with the dev build of R have been fixed. In particular, `fst` now builds correctly with the Clang 6.0 toolchain which will be released by CRAN shortly (thanks @kevinushey for reporting the problem and CRAN maintainers for the advance warning.

* An error was fixed where compressing a short factor column with 128 to 32767 levels but only a single value, returned incorrect results (issue #128, thanks @martinblostein for reporting and help fixing the problem).

* An error was fixed where columns f type 'ITime' were incorrectly serialized (issue #126, thanks @Giqles for reporting the problem).

* An error was fixed where using `fst` as a dependency in another package and building that package in RStudio, crashed RStudio. The problem was that RStudio uses a fork to build or document a package. That fork made `fst` use OpenMP library methods, which leads to crashes on macOS. After the fix, no calls to any OpenMP library method are now made from `fst` when it's run from a forked process (issue #100 and issue #109, thanks to @eipi10, @PeteHaitch, @kevinushey, @thierrygosselin, @xiaodaigh and @jzzcutler for reporting the problem and help fix it).

## Documentation

* Documentation for method `write_fst` was improved (issue #123, thanks @krlmlr for reporting and submitting a pull request).


# fst 0.8.2

## New features

* Package `fst` has support for multi-threading using OpenMP. Compression, decompression and disk IO have been largely parallelized for (much) improved performance.

* Many new column types are now supported by the `fst` format (where appropriate, both the `double` and `integer` variants are supported):

    * `raw`
    * `DateTime`
    * `integer64`
    * `nanotime`
    * `POSIXct`
    * `ordered factors`
    * `difftime`
    
    _Thanks @arunsrinivasan, @derekholmes, @phillc73, @HughParsonage, @statquant, @eddelbuettel, @eipi10, and @verajosemanuel for feature requests and helpful discussions._

* Multi-threaded `LZ4` and `ZSTD` compression using methods `compress_fst` and `decompress_fst`. These methods provide a direct API to the `LZ4` and `ZSTD` compressors at speeds of multiple GB/s. A specific block format is used to facilitate parallel processing. For additional stability, hashes can be calculated if required.

* Method `hash_fst` provides an extremely fast multi-threaded 64-bit hashing algorithm based on `xxHash`. Speeds up to the memory bandwidth can be achieved.

* Faster conversion to `data.table` in `read_fst`. _Thanks @dselivanov_

* Package `data.table` is now an optional dependency. _Thanks @jimhester_. Note that in the near future, a dependency on `data.table` will probably be introduced again, as `fst` will get a `data.table`-like interface.

* The `fst` format has a _magic number_ to be able to identify a `fst` file without actually opening the file or requiring the `fstlib` library. _Thanks @davidanthoff._

* For development versions, the build number is now shown when fst is loaded. _Thanks @skanskan._

* Character encodings are preserved for character and factor columns. _Thanks @carioca67 and @adrianadermon_

* Naming of fst methods is now consistent. _Thanks @jimhester and @brinkhuis._

* The core C++ code with the API to read and write `fst` files, and use compression and hashing now lives in a separate library called [`fstlib`](https://github.com/fstpackage/fstlib). Although not visible to the user, this is a major development allowing `fst` to be implemented for other languages than `R` (with comparable performance).

## Bugs solved

* Tilde-expansion in `write_fst` not correctly processed. _Thanks @HughParsonage, @PoGibas._

* Writing more than INT_MAX rows crashes `fst`. _Thanks @wei-wu-nyc_

* Incorrect fst file is created when an empty data.table is saved. _Thanks @wei-wu-nyc._

* Error/crash when saving factor column with 0 factor levels. _Thanks @martinblostein._

* No warning was given when disk runs out of space during a `fstwrite` operation.

* A data.table warning message was given on modification of columns of a sorted table. _Thanks @martinblostein._

* Stack imbalance warnings under certain conditions. _Thanks @ryankennedyio_

## Benchmarks

Thanks to @mattdowle, @st-pasha, @phillc73 for valuable discussions on `fst` benchmarks and how to accurately perform (and present) them.

## Additional credits

* Special thanks to @arunsrinivasan for a lot of valuable discussions on the future direction of the `fst` package, I hope `fst` may continue to benefit from your experience!

* Thanks for reporting and discussing various bugs, inconsistencies, instabilities or installation problems to @treysp, @wei-wu-nyc, @khsu15, @PMassicotte, @xiaodaigh, @renkun-ken, @statquant, @tgolden23, @carioca67, @jzzcutler, @MehranMoghtadai.

* And thanks to @mperone, @kendonB, @xiaodaigh, @derekholmes, @pmakai, @1beb, @BenoitLondon, @skanskan, @petermuller71, @nextpagesoft, @cawthm, @jeroenjanssens, @dselivanov, @Fpadt and @kbroman for helpful (online) discussions and (feature) requests. All the community feedback is much appreciated and tremendously helps to to improve the stability and usability of `fst`! (if I missed anyone, I apologize in advance, please let me know and I will fix this document ASAP)
