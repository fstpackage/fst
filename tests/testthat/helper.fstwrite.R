
# require(fstversion1)
# require(fst)


# Set default writer for testing
# To run all tests reading against an earlier version of the fst package, build that version with
# a different package name. Then call write.fst from that package namespace below as indicated
fstwrite <- function(x, path, compress = 0)
{
  fst::write.fst(x, path, compress)  # use current version of fst package
  # fstversion1::write.fst(x, path, compress)  # use current version of fst package
}


# Force current version of package for reading
fstread <- function(path, columns = NULL, from = 1, to = NULL, as.data.table = FALSE)
{
  fst::read.fst(path, columns, from, to, as.data.table)
}


