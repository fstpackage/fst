
**If you are viewing this file on CRAN, please check latest news on GitHub [here](https://github.com/fstpackage/fst/blob/develop/NEWS.md).**

### Changes in v0.8.0 (in development)

#### NEW FEATURES

1. Support for `raw` column type.
2. Support for `DateTime` column type.
3. Support for `integer64` column type.
4. Support for `nanotime` column type.
5. Added methods `fstcompress` and `fstdecompress` for in-memory data compression with LZ4 and ZSTD. This method uses a multi-threaded implementation with OpenMP to achieve high compression and decompression speeds.



#### Bug fixes

1. No warning was given when disk runs out of space during a `fstwrite` operation. 