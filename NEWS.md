
**If you are viewing this file on CRAN, please check latest news on GitHub [here](https://github.com/fstpackage/fst/blob/develop/NEWS.md).**

## Changes in v0.8.0

### NEW FEATURES

- Package `fst` now has support for OpenMP. This enables multi-threaded compression, decompression and disk IO for improved performance.

- Support added for various column types:
    - `raw` type
    - `DateTime` type
    - `integer64` type
    - `nanotime` type
    - `POSIXct` type.
    - `IDate` (`data.table`)
    - `ITime` (`data.table`)

- Methods `fstcompress` and `fstdecompress` for in-memory data compression using the `LZ4` and `ZSTD` compressors. These methods use a multi-threaded implementation with OpenMP to achieve high compression and decompression speeds..

- All columns except `character` columns are now processed in parallel using OpenMP for higher serialization and deserialization speeds.

- Header of the `fst` format is internally hashed for safety and stability. Any error in the file header will be detected by calculating the hash value of header data and comparing it with the previously stored value.

- The core of `fst` is now a `C++` library without any dependencies to the `R` framework. The `fst` package is a wrapper around this `C++` core library. This allows for future implementations in other languages (e.g. Python).

### Bug fixes

1. No warning was given when disk runs out of space during a `fstwrite` operation. 
