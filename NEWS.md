
**If you are viewing this file on CRAN, please check latest news on GitHub [here](https://github.com/fstpackage/fst/blob/develop/NEWS.md).**

### Changes in v0.8.0 (in development)

#### NEW FEATURES

* Package `fst` now has support for OpenMP. This enables multi-threaded compression, decompression and disk IO for (much) improved performance.
* Support for `raw` column type.
* Support for `DateTime` column type.
* Support for `integer64` column type.
* Support for `nanotime` column type.
* New methods `compress_fst` and `decompress_fst` allow in-memory data compression with `LZ4` and `ZSTD` compression algorithms. These methods uses a multi-threaded implementation (using OpenMP) to achieve high compression and decompression speeds. A specific block format is used to facilitate parallel processing.
* All column types except `character` columns are now processed in parallel using OpenMP for higher serialization speeds.
* Package `fst` now has a consistent public interface for all methods. The interface follows the `tidyverse` style which is enforced by `lintr` unit tests during package development:

    * `read_fst`
    * `write_fst`
    * `compress_fst`
    * `decompress_fst`
    * `hash_fst`
    * `metadata_fst`
    * `threads_fst`
* New method `hash_fst` allow the computation of a 64-bit hash value from `raw` input vectors. It uses a multi-threaded implementation of the `xxHash` algorithm for extreme speeds (at the memory speed limit).


#### Bug fixes

1. No warning was given when disk runs out of space during a `fstwrite` operation. 
