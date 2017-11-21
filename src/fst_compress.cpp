/*
 fst - R package for ultra fast storage and retrieval of datasets

 Copyright (C) 2017-present, Mark AJ Klik

 This file is part of the fst R package.

 The fst R package is free software: you can redistribute it and/or modify it
 under the terms of the GNU Affero General Public License version 3 as
 published by the Free Software Foundation.

 The fst R package is distributed in the hope that it will be useful, but
 WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 for more details.

 You should have received a copy of the GNU Affero General Public License along
 with the fst R package. If not, see <http://www.gnu.org/licenses/>.

 You can contact the author at:
 - fst R package source repository : https://github.com/fstpackage/fst
*/


#include <memory>
#include <cstring>

#include <Rcpp.h>

#include <interface/fstcompressor.h>
#include <interface/fsthash.h>

#include <fst_type_factory.h>
#include <fst_error.h>

// Calculate the 64-bit xxHash of a raw vector using a default or
// custom hash seed
SEXP fsthasher(SEXP rawVec, SEXP seed, SEXP blockHash)
{
  FstHasher hasher;

  SEXP res = PROTECT(Rf_allocVector(INTSXP, 2));

  bool bHash = false;

  // blockHash == FALSE
  if (*LOGICAL(blockHash) == 1)
  {
    bHash = true;
  }

  int* uintP = INTEGER(res);

  unsigned long long hashResult = 0;

  // use default fst seed
  if (Rf_isNull(seed))
  {
    hashResult = hasher.HashBlob((unsigned char*) RAW(rawVec), Rf_length(rawVec), bHash);
    std::memcpy(uintP, &hashResult, 8);

    UNPROTECT(1);
    return res;
  }

  uintP[0] = 2;
  uintP[1] = 2;

  UNPROTECT(1);
  return res;

  // use custom seed
  hashResult = hasher.HashBlobSeed((unsigned char*) RAW(rawVec), Rf_length(rawVec),
    *((unsigned int*) INTEGER(seed)), bHash);
  std::memcpy(uintP, &hashResult, 8);

  UNPROTECT(1);
  return res;
}


SEXP fstcomp(SEXP rawVec, SEXP compressor, SEXP compression, SEXP hash)
{
  std::unique_ptr<TypeFactory> typeFactoryP(new TypeFactory());
  COMPRESSION_ALGORITHM algo;

  if (!Rf_isLogical(hash))
  {
    Rf_error("Please specify true of false for parameter hash.");
  }

  if (Rf_NonNullStringMatch(STRING_ELT(compressor, 0), Rf_mkChar("LZ4")))
  {
    algo = COMPRESSION_ALGORITHM::ALGORITHM_LZ4;
  } else if (Rf_NonNullStringMatch(STRING_ELT(compressor, 0), Rf_mkChar("ZSTD")))
  {
    algo = COMPRESSION_ALGORITHM::ALGORITHM_ZSTD;
  } else
  {
    return fst_error("Unknown compression algorithm selected");
  }

  FstCompressor fstcompressor(algo, *INTEGER(compression), (ITypeFactory*) typeFactoryP.get());

  unsigned long long vecLength = Rf_xlength(rawVec);
  unsigned char* data = (unsigned char*) RAW(rawVec);

  std::unique_ptr<IBlobContainer> blobContainerP;

  try
  {
    blobContainerP = std::unique_ptr<IBlobContainer>(fstcompressor.CompressBlob(data, vecLength, *LOGICAL(hash)));
  }
  catch(const std::runtime_error& e)
  {
    return fst_error(e.what());
  }
  catch ( ... )
  {
    return fst_error("Unexpected error detected while compressing data.");
  }

  SEXP resVec = ((BlobContainer*)(blobContainerP.get()))->RVector();

  return resVec;
}


SEXP fstdecomp(SEXP rawVec)
{
  std::unique_ptr<ITypeFactory> typeFactoryP(new TypeFactory());

  FstCompressor fstcompressor((ITypeFactory*) typeFactoryP.get());

  unsigned long long vecLength = Rf_xlength(rawVec);
  unsigned char* data = (unsigned char*) (RAW(rawVec));

  std::unique_ptr<BlobContainer> resultContainerP;

  try
  {
    resultContainerP = std::unique_ptr<BlobContainer>(static_cast<BlobContainer*>(fstcompressor.DecompressBlob(data, vecLength)));
  }
  catch(const std::runtime_error& e)
  {
    return fst_error(e.what());
  }
  catch ( ... )
  {
    return fst_error("Error detected while decompressing data.");
  }

  SEXP resVec = resultContainerP->RVector();

  return resVec;
}

