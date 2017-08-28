
context("in-memory compression")


# Sample raw vector
RawVec <- function(length) {
  as.raw(sample(1:10, length, replace = TRUE))
}

rawVec <- RawVec(100)  # vector size less than single block


test_that("interface for compressing raw vectors", {
  y <- fstcompress(rawVec)
  expect_equal(typeof(y), "raw")

  expect_error(fstcompress(5), "Parameter x is not set to a raw vector")

  expect_error(fstcompress(rawVec, 4), "Parameter compressor should be set")

  expect_error(fstcompress(rawVec, compression = TRUE), "Parameter compression should be a numeric value")

  expect_error(fstcompress(rawVec[0], compressor = "LZ4", compression = 0), "Source contains no data")
})


# rest write / read cycle for single vector and compression settings
TestRoundCycle <- function(vec, compressor, compression) {
  y <- fstcompress(rawVec, compressor, compression)
  z <- fstdecompress(y)
  expect_equal(rawVec, z, info = paste("compressor:", compressor, "compression:", compression))  # return type

  y
}


# rest write / read cycle for multiple compression settings
TestVec <- function(rawVec) {
  TestRoundCycle(rawVec, "LZ4", 0)
  TestRoundCycle(rawVec, "LZ4", 100)
  TestRoundCycle(rawVec, "ZSTD", 0)
  TestRoundCycle(rawVec, "ZSTD", 100)
}


test_that("compress round cycle small vector", {

  # single core
  fstsetthreads(1)
  comp_result1 <- TestVec(rawVec)

  # dual core (max on CRAN)
  fstsetthreads(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle single block", {

  # fstcompress prefers 48 block of at least 16384 bytes (16 kB)
  rawVec <- RawVec(16384)  # exactly single block

  # single core single block
  fstsetthreads(1)
  comp_result1 <- TestVec(rawVec)

  # only 1 core will be active on a single block
  fstsetthreads(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle around single block", {

  # fstcompress prefers 48 block of at least 16384 bytes (16 kB)
  rawVec <- RawVec(16383)  # exactly single block minus 1

  # single core single block
  fstsetthreads(1)
  comp_result1 <- TestVec(rawVec)

  # only 1 core will be active on a single block
  fstsetthreads(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)

  rawVec <- RawVec(16385)  # exactly single block plus 1

  # single core single block
  fstsetthreads(1)
  comp_result1 <- TestVec(rawVec)

  # 2 cores will be active on two blocks
  fstsetthreads(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle multiple blocks per thread", {

  rawVec <- RawVec(100000)

  fstsetthreads(1)
  comp_result1 <- TestVec(rawVec)

  fstsetthreads(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


test_that("compress round cycle blocksize larger than 16kB", {

  rawVec <- RawVec(1000000)

  fstsetthreads(1)
  comp_result1 <- TestVec(rawVec)

  fstsetthreads(2)
  comp_result2 <- TestVec(rawVec)

  # result independent of the number of blocks used
  expect_equal(comp_result1, comp_result2)
})


rawVec <- RawVec(50000)  # 3 blocks


test_that("erroneous compressed data", {

  z <- fstcompress(rawVec, compressor = "LZ4", compression = 100)  # note: very bad compression ratio

  # crash here -> hash the index and check hash for safety

  y <- z
  y[41:48] <- as.raw(0L)  # mess up second block offset
  expect_error(fstdecompress(y), "An error was detected in the compressed data stream")

  y <- z
  y[17:24] <- as.raw(0L)  # set vector length to zero
  expect_error(fstdecompress(y), "Compressed data vector has incorrect size")

  y <- z
  y[17:24] <- as.raw(100L)  # set vector length to very big value
  expect_error(fstdecompress(y), "Compressed data vector has incorrect size")

  y <- z
  y[17:24] <- as.raw(c(0L, 0L, 0L, 1L, 0L, 0L, 0L, 0L))  # set vector length to very big value
  expect_error(fstdecompress(y), "Compressed data vector has incorrect size")

  y <- fstdecompress(z)
  expect_equal(rawVec, y)  # return type

  y <- fstcompress(rawVec, compressor = "ZSTD", compression = 0)
  z <- fstdecompress(y)
  expect_equal(rawVec, z)  # return type

  y <- fstcompress(rawVec, compressor = "ZSTD", compression = 100)
  z <- fstdecompress(y)
  expect_equal(rawVec, z)  # return type
})
