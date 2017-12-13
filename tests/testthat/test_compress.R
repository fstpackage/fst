
context("in-memory compression")


# Sample raw vector
RawVec <- function(length) {
  as.raw(sample(1:10, length, replace = TRUE))
}

rawVec <- RawVec(100)  # vector size less than single block


test_that("interface for compressing raw vectors", {
  y <- compress_fst(rawVec)
  expect_equal(typeof(y), "raw")

  expect_error(compress_fst(5), "Parameter x is not set to a raw vector")

  expect_error(compress_fst(rawVec, 4), "Parameter compressor should be set")

  expect_error(compress_fst(rawVec, compression = TRUE), "Parameter compression should be a numeric value")

  expect_error(compress_fst(rawVec[0], compressor = "LZ4", compression = 0), "Source contains no data")
})


# rest write / read cycle for single vector and compression settings
TestRoundCycle <- function(vec, compressor, compression) {
  y <- compress_fst(rawVec, compressor, compression)
  z <- decompress_fst(y)

  expect_equal(rawVec, z, info = paste("compressor:", compressor, "compression:", compression))  # return type

  y
}


# rest write / read cycle for multiple compression settings
TestVec <- function(rawVec) {
  y1 <- TestRoundCycle(rawVec, "LZ4", 0)
  y2 <- TestRoundCycle(rawVec, "LZ4", 100)
  y3 <- TestRoundCycle(rawVec, "ZSTD", 0)
  y4 <- TestRoundCycle(rawVec, "ZSTD", 100)

  list(y1, y2, y3, y4)
}


test_that("compress round cycle small vector", {
  # single core
  threads_fst(1)
  comp_result1 <- TestVec(rawVec)

  # dual core (max on CRAN)
  threads_fst(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle single block", {
  # compress_fst prefers 48 block of at least 16384 bytes (16 kB)
  rawVec <- RawVec(16384)  # exactly single block

  # single core single block
  threads_fst(1)
  comp_result1 <- TestVec(rawVec)

  # only 1 core will be active on a single block
  threads_fst(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle around single block", {
  # compress_fst prefers 48 block of at least 16384 bytes (16 kB)
  rawVec <- RawVec(16383)  # exactly single block minus 1

  # single core single block
  threads_fst(1)
  comp_result1 <- TestVec(rawVec)

  # only 1 core will be active on a single block
  threads_fst(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)

  rawVec <- RawVec(16385)  # exactly single block plus 1

  # single core single block
  threads_fst(1)
  comp_result1 <- TestVec(rawVec)

  # 2 cores will be active on two blocks
  threads_fst(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle multiple blocks per thread", {
  rawVec <- RawVec(100000)

  threads_fst(1)
  comp_result1 <- TestVec(rawVec)

  threads_fst(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle blocksize larger than 16kB", {
  rawVec <- RawVec(1000000)

  threads_fst(1)
  comp_result1 <- TestVec(rawVec)

  threads_fst(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


rawVec <- RawVec(50000)  # 4 blocks


test_that("erroneous compressed data", {
  # Test LZ4 compressor
  z <- compress_fst(rawVec, compressor = "LZ4", compression = 100)  # note: very bad compression ratio

  y <- z
  y[41:48] <- as.raw(0L)  # mess up second block offset
  expect_error(decompress_fst(y), "Incorrect header information found")

  y <- z
  y[17:24] <- as.raw(0L)  # set vector length to zero
  expect_error(decompress_fst(y), "Incorrect header information found")

  y <- decompress_fst(z)
  expect_equal(rawVec, y)  # return type

  # Test ZSTD compressor
  y <- compress_fst(rawVec, compressor = "ZSTD", compression = 0)
  z <- decompress_fst(y)
  expect_equal(rawVec, z)  # return type

  y <- compress_fst(rawVec, compressor = "ZSTD", compression = 100)  # maximum compression
  z <- decompress_fst(y)
  expect_equal(rawVec, z)  # return type

  # Mess up compressed data block
  # Header has length 76, so data of first block starts at byte 77

  # This error is catched by ZSTD
  y[77] <- as.raw(0L)  # set vector length to zero
  expect_error(decompress_fst(y), "An error was detected in the compressed data stream")

  # If using block hashes, erro is catched by fst
  y <- compress_fst(rawVec, compressor = "ZSTD", compression = 100, hash = TRUE)  # hash data blocks
  y[77] <- as.raw(0L)  # set vector length to zero
  expect_error(decompress_fst(y), "Incorrect input vector")
})


test_that("hash can use custom seed", {
  hash1 <- hash_fst(rawVec)
  hash2 <- hash_fst(rawVec, 345345)

  # alter vector in two places
  rawVec[100] <- as.raw( (as.integer(rawVec[100]) + 2) %% 256)
  rawVec[200] <- as.raw( (as.integer(rawVec[200]) + 2) %% 256)
  hash3 <- hash_fst(rawVec)

  expect_true(sum(hash1 != hash2) == 2)
  expect_true(sum(hash1 != hash3) == 2)
})


test_that("block_hash can be set", {
  hash1 <- hash_fst(rawVec)

  # larger than 1 block raw vectors give different results
  hash4 <- hash_fst(rawVec, block_hash = FALSE)  # single threaded hash
  expect_true(sum(hash1 != hash4) == 2)

  small_raw_vec <- as.raw(rep(0, 10))
  hash1 <- hash_fst(small_raw_vec)

  # smaller than 1 block raw vectors give identical results
  hash4 <- hash_fst(small_raw_vec, block_hash = FALSE)  # single threaded hash
  expect_true(sum(hash1 != hash4) == 0)
})



test_that("argument error", {
  expect_error(compress_fst(1), "Parameter x is not set to a raw vector")

  expect_error(hash_fst(as.raw(1), "no integer"), "Please specify an integer value for the hash seed")

  expect_error(hash_fst(1), "Please specify a raw vector as input parameter x")

  expect_error(hash_fst(as.raw(1), block_hash = 1), "Please specify a logical value for parameter block_hash")

})
