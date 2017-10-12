
**If you are viewing this file on CRAN, please check latest news on GitHub [here](https://github.com/fstpackage/fst/blob/develop/NEWS.md).**

### Changes in v0.8.0 (in development)

#### NEW FEATURES

- Package `fst` now has support for OpenMP. This enables multi-threaded compression, decompression and disk IO for (much) improved performance.
-  Support for `raw` column type.
-  Support for `DateTime` column type.
-  Support for `integer64` column type.
-  Support for `nanotime` column type.
-  Added methods `fstcompress` and `fstdecompress` for in-memory data compression with LZ4 and ZSTD. This method uses a multi-threaded implementation with OpenMP to achieve high compression and decompression speeds.
- All columns except `character` columns are now processes in parallel using OpenMP for higher serialization speeds.

#### Bug fixes

1. No warning was given when disk runs out of space during a `fstwrite` operation. 