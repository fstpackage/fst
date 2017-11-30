
**If you are viewing this file on CRAN, please check latest news on GitHub [here](https://github.com/fstpackage/fst/blob/develop/NEWS.md).**

### Changes in v0.8.0 (in development)

#### New features

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

#### Bugs solved

* Tilde-expansion in `write_fst` not correctly processed. _Thanks @HughParsonage, @PoGibas._

* Writing more than INT_MAX rows crashes `fst`. _Thanks @wei-wu-nyc_

* Incorrect fst file is created when an empty data.table is saved. _Thanks @wei-wu-nyc._

* Error/crash when saving factor column with 0 factor levels. _Thanks @martinblostein._

* No warning was given when disk runs out of space during a `fstwrite` operation.

* Stack imbalance warnings under centain conditions. _Thanks @ryankennedyio_

#### Benchmarks

Thanks to @mattdowle, @st-pasha, @phillc73 for valuable discussions on `fst` benchmarks and how to accurately perform (and present) them.

#### Additional credits

* Special thanks to @arunsrinivasan for a lot of valuable discussions on the future direction of the `fst` package, I hope `fst` may continue to benefit from your experience!

* Thanks for reporting and discussing various bugs, inconsistencies, instabilities or installation problems to @treysp, @wei-wu-nyc, @khsu15, @PMassicotte, @xiaodaigh, @renkun-ken, @statquant, @tgolden23, @carioca67, @jzzcutler, @MehranMoghtadai.

* And thanks to @mperone, @kendonB, @xiaodaigh, @derekholmes, @pmakai, @1beb, @BenoitLondon, @skanskan, @petermuller71, @nextpagesoft, @cawthm, @jeroenjanssens, @dselivanov, @Fpadt and @kbroman for helpful (online) discussions and (feature) requests. All the community feedback is much appreciated and tremendously helps to to improve the stability and usability of `fst`! (if I missed anyone, I apologize in advance, please let me know and I will fix this document ASAP)
