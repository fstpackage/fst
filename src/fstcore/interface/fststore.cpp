/*
  fst - An R-package for ultra fast storage and retrieval of datasets.
  Copyright (C) 2017, Mark AJ Klik

  BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

  * Redistributions of source code must retain the above copyright
    notice, this list of conditions and the following disclaimer.

  * Redistributions in binary form must reproduce the above
    copyright notice, this list of conditions and the following disclaimer
    in the documentation and/or other materials provided with the
    distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
    A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
    OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
    SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
    LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
    DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
    THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact the author at :
  - fst source repository : https://github.com/fstPackage/fst
*/


#include <fstream>
#include <vector>
#include <stdexcept>

#include <fstdefines.h>
#include <fsttable.h>
#include <fststore.h>
#include <fstmetadata.h>


using namespace std;


FstStore::FstStore(std::string fstFile)
{
  this->fstFile = fstFile;
  this->append = false;
}


FstStore::FstStore(std::string fstFile, bool append)
{
  this->fstFile = fstFile;
  this->append = append;
}

//
// void FstStore::ColBind(FstTable table)
// {
//   // fst file stream using a stack buffer
//   ifstream myfile;
//   char ioBuf[4096];
//   myfile.rdbuf()->pubsetbuf(ioBuf, 4096);
//   myfile.open(fstFile.c_str(), ios::binary);
//
//   const std::vector<FstColumn*> columns = table.Columns();
//
//   if (myfile.fail())
//   {
//     myfile.close();
//     throw std::runtime_error(FSTERROR_ERROR_OPENING_FILE);
//   }
//
//   // Read fst file header
//   FstMetaData metaData;
//
//   unsigned int tableClassType;
//   int keyLength, nrOfColsFirstChunk;
//   unsigned int version = metaData.ReadHeader(myfile, tableClassType, keyLength, nrOfColsFirstChunk);
//
//   // We can't append to a fst file with version lower than 1
//   if (version == 0)
//   {
//     myfile.close();
//     throw std::runtime_error(FSTERROR_NO_APPEND);
//   }
//
//
//   // Collect meta data from horizontal chunk headers and last vertical chunk set
//   uint64_t firstHorzChunkPos = 24 + 4 * keyLength;  // hard-coded offset: beware of format changes
//   istream* fstfile  = static_cast<istream*>(&myfile);
//   metaData.Collect(*fstfile, firstHorzChunkPos);
//
//   vector<uint16_t> colTypeVec = metaData.colTypeVec;
//   uint64_t nrOfRowsPrev = metaData.nrOfRows;
//
//   // We can only append an equal amount of columns
//   if (colTypeVec.size() != columns.size())
//   {
//     myfile.close();
//     throw std::runtime_error(FSTERROR_INCORRECT_COL_COUNT);
//   }
//
//    // check for equal column types
//
//   // set compression or copy from existing meta data
//
//   // Seek to last horizontal chunk index
//
//   // Seek to last vertical chunk
//
//   // Add chunk
//
//   // Update vertical chunk index of lat horizontal chunk
//
// }
//

