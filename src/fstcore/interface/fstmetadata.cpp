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

#include <fstmetadata.h>
#include <fstdefines.h>
#include "character_v6.h"


//  8                      | unsigned long long | nextHorzChunkSet
//  8                      | unsigned long long | nextVertChunkSet
//  8                      | unsigned long long | nrOfRows
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | nrOfCols
//  2 * nrOfCols           | unsigned short int | colAttributesType (not implemented yet)
//  2 * nrOfCols           | unsigned short int | colTypes
//  ?                      | char               | colNames

using namespace std;


// Read header information
unsigned int FstMetaData::ReadHeader(istream &fstfile, unsigned int &tableClassType, int &keyLength, int &nrOfColsFirstChunk)
{
  // Get meta-information for table
  char tableMeta[TABLE_META_SIZE];
  fstfile.read(tableMeta, TABLE_META_SIZE);

  if (!fstfile)
  {
    throw std::runtime_error(FSTERROR_DAMAGED_HEADER);
  }


  unsigned long long* p_fstFileID = (unsigned long long*) tableMeta;
  unsigned int* p_table_version   = (unsigned int*) &tableMeta[8];
  unsigned int* p_tableClassType  = (unsigned int*) &tableMeta[12];
  int* p_keyLength                = (int*) &tableMeta[16];
  int* p_nrOfColsFirstChunk       = (int*) &tableMeta[20];


  keyLength          = *p_keyLength;
  nrOfColsFirstChunk = *p_nrOfColsFirstChunk;

  // Without a proper file ID, we may be looking at a fst v0.7.2 file format
  if (*p_fstFileID != FST_FILE_ID)
  {
    return 0;
  }

  // Compare file version with current
  if (*p_table_version > FST_VERSION)
  {
    throw "Incompatible fst file: file was created by a newer version of the fst package.";
  }

  return *p_table_version;
}


int FstMetaData::Collect(istream &fstfile, uint64_t filePointer)
{
  // Clear info

  this->colAttrVec.clear();
  this->colNameVec.clear();
  this->colTypeVec.clear();

  nrOfRows = 0;

  return CollectRecursive(fstfile, filePointer);
}


int FstMetaData::CollectRecursive(std::istream &fstfile, uint64_t filePointer)
{
  // Horizontal chunk meta data

  int headerSize = 32;
  char horzChunkInfo[headerSize];
  fstfile.seekg(filePointer);
  fstfile.read(horzChunkInfo, headerSize);

  unsigned long long* p_nextHorzChunkSet = (unsigned long long*) horzChunkInfo;
  unsigned long long* p_nextVertChunkSet = (unsigned long long*) &horzChunkInfo[8];
  unsigned long long* p_nrOfRows         = (unsigned long long*) &horzChunkInfo[16];
  unsigned int* p_version                = (unsigned int*) &horzChunkInfo[24];
  int* p_nrOfCols                        = (int*) &horzChunkInfo[28];

  int nrOfCols = *p_nrOfCols;

  // Compare file version with current
  if (*p_version > FST_VERSION) throw 1;

  // Column information
  int metaSize = 4 * nrOfCols;
  char* metaDataBlock = new char[metaSize];
  fstfile.read(metaDataBlock, metaSize);

  unsigned short int* colAttributeTypes  = (unsigned short int*) metaDataBlock;
  unsigned short int* colTypes           = (unsigned short int*) &metaDataBlock[2 * nrOfCols];

  // Column names TODO: this method should return a vector<string>
  SEXP colNames;
  PROTECT(colNames = Rf_allocVector(STRSXP, nrOfCols));
  unsigned long long offset = filePointer + headerSize + 4 * nrOfCols;
  fdsReadCharVec_v6(fstfile, colNames, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);

  // Populate meta data (with left to right column data)
  for (int colCount = 0; colCount < nrOfCols; ++colCount)
  {
    const char* strElem = CHAR(STRING_ELT(colNames, colCount));
    colNameVec.push_back(string(strElem));
    colAttrVec.push_back(colAttributeTypes[colCount]);
    colTypeVec.push_back(colTypes[colCount]);
  }

  UNPROTECT(1);

  // Still horizontal chunks left
  if (*p_nextHorzChunkSet != 0)
  {
    int errorRes = this->CollectRecursive(fstfile, *p_nextHorzChunkSet);
    if (errorRes != 0) return errorRes;
  }

  // No horizontal chunks left, get information from last chunk set
  nrOfRows = *p_nrOfRows;

  if (*p_nextVertChunkSet != 0)
  {
    // count total number of rows for all data chunks
    throw std::runtime_error(FSTERROR_NOT_IMPLEMENTED);
  }

  lastHorzChunkPointer = filePointer;

  return 0;
}


