/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  fstlib is free software: you can redistribute it and/or modify it under the
  terms of the GNU Affero General Public License version 3 as published by the
  Free Software Foundation.

  fstlib is distributed in the hope that it will be useful, but WITHOUT ANY
  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
  A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
  details.

  You should have received a copy of the GNU Affero General Public License
  along with fstlib. If not, see <http://www.gnu.org/licenses/>.

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/


#include <iostream>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <algorithm>
#include <memory>

#include <interface/istringwriter.h>
#include <interface/ifsttable.h>
#include <interface/icolumnfactory.h>
#include <interface/fstdefines.h>
#include <interface/fststore.h>

#include <character/character_v6.h>
#include <factor/factor_v7.h>
#include <integer/integer_v8.h>
#include <double/double_v9.h>
#include <logical/logical_v10.h>
#include <integer64/integer64_v11.h>
#include <byte/byte_v12.h>

#include <LZ4/xxhash.h>

using namespace std;


// Table header [node A] [size: 48]
//
//  8                      | unsigned long long | hash value         // hash of table header
//  4                      | unsigned int       | FST_VERSION        // table header fstcore version
//  4                      | int                | table flags        // binary table flags
//  8                      |                    | free bytes         // possible future use
//  4                      | unsigned int       | FST_VERSION_MAX    // minimum fstcore version required
//  4                      | int                | nrOfCols           // total number of columns in primary chunkset
//  8                      | unsigned long long | primaryChunkSetLoc // reference to the table's primary chunkset
//  4                      | int                | keyLength          // number of keys in table
//  4                      | int                | fst magic number   // signature of every fst file

// Table flag specification:
//
// bit 0                   | endianess          | if true, integers are stored in little endian format

// Key index vector (only needed when keyLength > 0) [attached leaf of A] [size: 8 + 4 * (keyLength + keyLength % 2]
//
//  8                      | unsigned long long | hash value         // hash of key index vector (if present)
//  4 * keyLength          | int                | keyColPos          // key column indexes in the first horizontal chunk
//  ?                      | int                | free bytes         // possibly free bytes (for 8-byte allignment)

// Chunkset header [node C, free leaf of A or other chunkset header] [size: 80 + 8 * nrOfCols]
//
//  8                      | unsigned long long | hash value         // hash of chunkset header
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | chunkset flags     // binary horizontal chunk flags
//  8                      |                    | free bytes         // possible future use
//  8                      |                    | free bytes         // possible future use
//  8                      | unsigned long long | colNamesPos        // reference to column names vector
//  8                      | unsigned long long | nextHorzChunkSet   // reference to next chunkset header (additional columns)
//  8                      | unsigned long long | primChunksetIndex  // reference to primary chunkset data (nrOfCols columns)
//  8                      | unsigned long long | secChunksetIndex   // reference to primary chunkset data (nrOfCols columns)
//  8                      | unsigned long long | nrOfRows           // total number of rows in chunkset
//  4                      | int                | p_nrOfChunksetCols // number of columns in primary chunkset
//  4                      |                    | free bytes         // possible future use
//  2 * nrOfCols           | unsigned short int | colAttributesType  // column attributes
//  2 * nrOfCols           | unsigned short int | colTypes           // column types
//  2 * nrOfCols           | unsigned short int | colBaseTypes       // column base types
//  2 * nrOfCols           | unsigned short int | colScales          // column scales (pico, nano, micro, milli, kilo, mega, giga, tera etc.)

// Column names [leaf to C]  [size: 24 + x]
//
//  8                      | unsigned long long | hash value         // hash of column names header
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | colNames flags     // binary horizontal chunk flags
//  8                      |                    | free bytes         // possible future use
//  x                      | char               | colNames           // column names (internally hashed)

// Chunk index [node D, leaf of C] [size: 96]
//
//  8                      | unsigned long long | hash value         // hash of chunkset data header
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | index flags        // binary horizontal chunk flags
//  8                      |                    | free bytes         // possible future use
//  2                      | unsigned int       | nrOfChunkSlots     // number of chunk slots
//  6                      |                    | free bytes         // possible future use
//  8 * 4                  | unsigned long long | chunkPos           // data chunk addresses
//  8 * 4                  | unsigned long long | chunkRows          // data chunk number of rows

// Data chunk header [node E, leaf of D] [size: 24 + 8 * nrOfCols]
//
//  8                      | unsigned long long | hash value         // hash of chunkset data header
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | data chunk flags
//  8                      |                    | free bytes         // possible future use
//  8 * nrOfCols           | unsigned long long | positionData       // columnar position data
//

// Column data blocks [leaf of E]
//  y                      |                    | column data        // data blocks with column element values


FstStore::FstStore(std::string fstFile)
{
  this->fstFile       = fstFile;
  this->blockReader   = nullptr;
  this->keyColPos     = nullptr;
  this->p_nrOfRows    = nullptr;
  metaDataBlock       = nullptr;
}


/**
 * \brief Read header information from a fst file
 * \param myfile a stream to a fst file
 * \param keyLength the number of key columns (output)
 * \param nrOfColsFirstChunk the number of columns in the first chunkset (output)
 * \return 
 */
inline unsigned int ReadHeader(ifstream &myfile, int &keyLength, int &nrOfColsFirstChunk)
{
  // Get meta-information for table
  char tableMeta[TABLE_META_SIZE];
  myfile.read(tableMeta, TABLE_META_SIZE);

  if (!myfile)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_ERROR_OPEN_READ));
  }

  unsigned long long* p_headerHash         = reinterpret_cast<unsigned long long*>(tableMeta);
  //unsigned int* p_tableVersion           = reinterpret_cast<unsigned int*>(&tableMeta[8]);
  //int* p_tableFlags                      = reinterpret_cast<int*>(&tableMeta[12]);
  //unsigned long long* p_freeBytes1       = reinterpret_cast<unsigned long long*>(&tableMeta[16]);
  unsigned int* p_tableVersionMax          = reinterpret_cast<unsigned int*>(&tableMeta[24]);
  int* p_nrOfCols                          = reinterpret_cast<int*>(&tableMeta[28]);
  //unsigned long long* primaryChunkSetLoc = reinterpret_cast<unsigned long long*>(&tableMeta[32]);
  int* p_keyLength                         = reinterpret_cast<int*>(&tableMeta[40]);
  //char* p_freeBytes                      = reinterpret_cast<char*>(&tableMeta[44]);

  // check header hash
  const unsigned long long hHash = XXH64(&tableMeta[8], TABLE_META_SIZE - 8, FST_HASH_SEED);  // skip first 8 bytes (hash value itself)

  if (hHash != *p_headerHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_NON_FST_FILE));
  }

  // Compare file version with current
  if (*p_tableVersionMax > FST_VERSION)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_UPDATE_FST));
  }

  keyLength          = *p_keyLength;
  nrOfColsFirstChunk = *p_nrOfCols;

  return *p_tableVersionMax;
}


inline void SetKeyIndex(vector<int> &keyIndex, const int keyLength, const int nrOfSelect, int* keyColPos, int* colIndex)
{
  for (int i = 0; i < keyLength; ++i)
  {
    int colSel = 0;

    for (; colSel < nrOfSelect; ++colSel)
    {
      if (keyColPos[i] == colIndex[colSel])  // key present in result
      {
        keyIndex.push_back(colSel);
        break;
      }
    }

    // key column not selected
    if (colSel == nrOfSelect) return;
  }
}


/**
 * \brief Write a dataset to a fst file
 * \param fstTable interface to a dataset
 * \param compress compression factor in the range 0 - 100 
 */
void FstStore::fstWrite(IFstTable &fstTable, const int compress) const
{
  // Meta on dataset
  const int nrOfCols =  fstTable.NrOfColumns();  // number of columns in table
  const int keyLength = fstTable.NrOfKeys();  // number of key columns in table

  if (nrOfCols == 0)
  {
    throw(runtime_error("Your dataset needs at least one column."));
  }


  const unsigned long long tableHeaderSize    = TABLE_META_SIZE;
  unsigned long long keyIndexHeaderSize = 0;

  if (keyLength != 0)
  {
    // size of key index vector and hash and a possible allignment correction
    keyIndexHeaderSize = (keyLength % 2) * 4 + 4 * (keyLength + 2);
  }

  const unsigned long long chunksetHeaderSize = CHUNKSET_HEADER_SIZE + 8 * nrOfCols;
  const unsigned long long colNamesHeaderSize = 24;

  // size of fst file header
  const unsigned long long metaDataSize   = tableHeaderSize + keyIndexHeaderSize + chunksetHeaderSize + colNamesHeaderSize;
  char * metaDataWriteBlock               = new char[metaDataSize];  // fst metadata
  std::unique_ptr<char[]> metaDataPtr     = std::unique_ptr<char[]>(metaDataWriteBlock);


  // Table header [node A] [size: 48]

  unsigned long long* p_headerHash        = reinterpret_cast<unsigned long long*>(metaDataWriteBlock);
  unsigned int* p_tableVersion            = reinterpret_cast<unsigned int*>(&metaDataWriteBlock[8]);
  int* p_tableFlags                       = reinterpret_cast<int*>(&metaDataWriteBlock[12]);
  unsigned long long* p_freeBytes1        = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[16]);
  unsigned int* p_tableVersionMax         = reinterpret_cast<unsigned int*>(&metaDataWriteBlock[24]);
  int* p_nrOfCols                         = reinterpret_cast<int*>(&metaDataWriteBlock[28]);
  unsigned long long* primaryChunkSetLoc  = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[32]);
  int* p_keyLength                        = reinterpret_cast<int*>(&metaDataWriteBlock[40]);
  int* p_fst_magic_number                 = reinterpret_cast<int*>(&metaDataWriteBlock[44]);

  // Key index vector (only needed when keyLength > 0) [attached leaf of A] [size: 8 + 4 * keyLength + (keyLength % 2) * 4]

  unsigned long long* p_keyIndexHash      = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[TABLE_META_SIZE]);
  int* keyColPos                          = reinterpret_cast<int*>(&metaDataWriteBlock[TABLE_META_SIZE + 8]);
  //int* p_freeBytesB                       = reinterpret_cast<int*>(&metaDataWriteBlock[56 + keyLength * 4]);  // 8-byte allignment

  // Chunkset header [node C, free leaf of A or other chunkset header] [size: 80 + 8 * nrOfCols]

  unsigned int offset = tableHeaderSize + keyIndexHeaderSize;
  unsigned long long* p_chunksetHash      = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset]);
  unsigned int* p_chunksetHeaderVersion   = reinterpret_cast<unsigned int*>(&metaDataWriteBlock[offset + 8]);
  int* p_chunksetFlags                    = reinterpret_cast<int*>(&metaDataWriteBlock[offset + 12]);
  unsigned long long* p_freeBytes2        = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 16]);
  unsigned long long* p_freeBytes3        = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 24]);
  unsigned long long* p_colNamesPos       = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 32]);

  unsigned long long* p_nextHorzChunkSet  = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 40]);
  unsigned long long* p_primChunksetIndex = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 48]);
  unsigned long long* p_secChunksetIndex  = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 56]);
  unsigned long long* p_nrOfRows          = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 64]);
  int* p_nrOfChunksetCols                 = reinterpret_cast<int*>(&metaDataWriteBlock[offset + 72]);
  int* p_freeBytes4                       = reinterpret_cast<int*>(&metaDataWriteBlock[offset + 76]);

  unsigned short int* colAttributeTypes   = reinterpret_cast<unsigned short int*>(&metaDataWriteBlock[offset + CHUNKSET_HEADER_SIZE]);
  unsigned short int* colTypes            = reinterpret_cast<unsigned short int*>(&metaDataWriteBlock[offset + CHUNKSET_HEADER_SIZE + 2 * nrOfCols]);
  unsigned short int* colBaseTypes        = reinterpret_cast<unsigned short int*>(&metaDataWriteBlock[offset + CHUNKSET_HEADER_SIZE + 4 * nrOfCols]);
  unsigned short int* colScales           = reinterpret_cast<unsigned short int*>(&metaDataWriteBlock[offset + CHUNKSET_HEADER_SIZE + 6 * nrOfCols]);

  // Column names [leaf to C]  [size: 24 + x]

  offset = offset + chunksetHeaderSize;
  unsigned long long* p_colNamesHash      = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset]);
  unsigned int* p_colNamesVersion         = reinterpret_cast<unsigned int*>(&metaDataWriteBlock[offset + 8]);
  int* p_colNamesFlags                    = reinterpret_cast<int*>(&metaDataWriteBlock[offset + 12]);
  unsigned long long* p_freeBytes5        = reinterpret_cast<unsigned long long*>(&metaDataWriteBlock[offset + 16]);

  // Determine integer endianness
  int endianTest = 0x01234567;
  const bool isLittleEndian = (*reinterpret_cast<uint8_t*>(&endianTest)) == 0x67;

  // Set table header parameters
  
  *p_tableFlags = 0;
  *p_tableVersion                   = FST_VERSION;

  *p_fst_magic_number               = FST_MAGIC_NUMBER;
  *p_freeBytes1                     = 0;
  *p_tableVersionMax                = FST_VERSION;

  if (isLittleEndian) *p_tableFlags = 1;

  *p_nrOfCols                       = nrOfCols;
  *primaryChunkSetLoc               = TABLE_META_SIZE + keyIndexHeaderSize;
  *p_keyLength                      = keyLength;

  *p_headerHash = XXH64(&metaDataWriteBlock[8], tableHeaderSize - 8, FST_HASH_SEED);

  // Set key index if present

  if (keyLength != 0)
  {
    fstTable.GetKeyColumns(keyColPos);
    *p_keyIndexHash = XXH64(&metaDataWriteBlock[tableHeaderSize + 8], keyIndexHeaderSize - 8, FST_HASH_SEED);
  }

  *p_chunksetHeaderVersion = FST_VERSION;
  *p_chunksetFlags         = 0;
  *p_freeBytes2            = 0;
  *p_freeBytes3            = 0;
  *p_colNamesPos           = 0;
  *p_nextHorzChunkSet      = 0;
  *p_primChunksetIndex     = 0;
  *p_secChunksetIndex      = 0;

  const unsigned long long nrOfRows = fstTable.NrOfRows();
  *p_nrOfRows                 = nrOfRows;
  *p_nrOfChunksetCols         = nrOfCols;
  *p_freeBytes4               = 0;

  // Set column names header

  *p_colNamesVersion       = FST_VERSION;
  *p_colNamesFlags         = 0;
  *p_freeBytes5            = 0;

  if (nrOfRows == 0)
  {
    throw(runtime_error(FSTERROR_NO_DATA));
  }

  // Create file buffer
  //const size_t bufsize = 4096;
  //char buf[bufsize];

  // Create file and set buffer
  ofstream myfile;
  //myfile.rdbuf()->pubsetbuf(nullptr, 0);
  //myfile.rdbuf()->pubsetbuf(buf, bufsize);  // set write buffer

  *p_colNamesHash = XXH64(p_colNamesVersion, colNamesHeaderSize - 8, FST_HASH_SEED);


  // Open file in binary mode
  myfile.open(fstFile.c_str(), ios::out | ios::binary);  // write stream only

  if (myfile.fail())
  {
    myfile.close();
    throw(runtime_error(FSTERROR_ERROR_OPEN_WRITE));
  }

  // Write table meta information
  myfile.write(metaDataWriteBlock, metaDataSize);  // table meta data

  // Serialize column names
  {
    std::unique_ptr<IStringWriter> blockRunnerP(fstTable.GetColNameWriter());
    IStringWriter* blockRunner = blockRunnerP.get();
    fdsWriteCharVec_v6(myfile, blockRunner, 0, blockRunner->Encoding());   // column names
  }

  // Size of chunkset index header plus data chunk header
  const unsigned long long chunkIndexSize = CHUNK_INDEX_SIZE + DATA_INDEX_SIZE + 8 * nrOfCols;
  char* chunkIndex = new char[chunkIndexSize];
  std::unique_ptr<char[]> chunkIndexPtr = std::unique_ptr<char[]>(chunkIndex);

  // Chunkset data index [node D, leaf of C] [size: 96]

  unsigned long long* p_chunkIndexHash = reinterpret_cast<unsigned long long*>(chunkIndex);
  unsigned int* p_chunkIndexVersion    = reinterpret_cast<unsigned int*>(&chunkIndex[8]);
  int* p_chunkIndexFlags               = reinterpret_cast<int*>(&chunkIndex[12]);
  unsigned long long* p_freeBytes6     = reinterpret_cast<unsigned long long*>(&chunkIndex[16]);
  unsigned short int* p_nrOfChunkSlots = reinterpret_cast<unsigned short int*>(&chunkIndex[24]);
  unsigned short int* p_freeBytes7     = reinterpret_cast<unsigned short int*>(&chunkIndex[26]);
  unsigned long long* p_chunkPos       = reinterpret_cast<unsigned long long*>(&chunkIndex[32]);
  unsigned long long* p_chunkRows      = reinterpret_cast<unsigned long long*>(&chunkIndex[64]);

  // Chunk data header [node E, leaf of D] [size: 24 + 8 * nrOfCols]

  unsigned long long* p_chunkDataHash  = reinterpret_cast<unsigned long long*>(&chunkIndex[96]);
  unsigned int* p_chunkDataVersion     = reinterpret_cast<unsigned int*>(&chunkIndex[104]);
  int* p_chunkDataFlags                = reinterpret_cast<int*>(&chunkIndex[108]);
  unsigned long long* p_freeBytes8     = reinterpret_cast<unsigned long long*>(&chunkIndex[112]);
  unsigned long long* positionData     = reinterpret_cast<unsigned long long*>(&chunkIndex[120]);  // column position index


  // Set chunkset data index header parameters

  *p_chunkIndexVersion = FST_VERSION;
  *p_chunkIndexFlags   = 0;
  *p_freeBytes6        = 0;
  *p_nrOfChunkSlots    = 4;
   p_freeBytes7[0]     = p_freeBytes7[1] = p_freeBytes7[2] = 0;
  *p_chunkRows         = nrOfRows;

  // Set data chunk header parameters

  *p_chunkDataVersion = FST_VERSION;
  *p_chunkDataFlags   = 0;
  *p_freeBytes8       = 0;


  // Row and column meta data
  myfile.write(chunkIndex, chunkIndexSize);   // file positions of column data


  // column data
  for (int colNr = 0; colNr < nrOfCols; ++colNr)
  {
    positionData[colNr] = myfile.tellp();  // current location
  	FstColumnAttribute colAttribute;
  	std::string annotation = "";
    short int scale = 0;
    bool hasAnnotation;

  	// get type and add annotation
    const FstColumnType colType = fstTable.ColumnType(colNr, colAttribute, scale, annotation, hasAnnotation);

    colBaseTypes[colNr] = static_cast<unsigned short int>(colType);
  	colAttributeTypes[colNr] = static_cast<unsigned short int>(colAttribute);
    colScales[colNr] = scale;

    switch (colType)
    {
      case FstColumnType::CHARACTER:
      {
        colTypes[colNr] = 6;
        std::unique_ptr<IStringWriter> stringWriterP(fstTable.GetStringWriter(colNr));
     		IStringWriter* stringWriter = stringWriterP.get();  // TODO: keep writer as part of fstTable (don't create)
        fdsWriteCharVec_v6(myfile, stringWriter, compress, stringWriter->Encoding());   // column names
        break;
      }

      case FstColumnType::FACTOR:
      {
        colTypes[colNr] = 7;
        int* intP = fstTable.GetIntWriter(colNr);  // level values pointer

        std::unique_ptr<IStringWriter> stringWriterP(fstTable.GetLevelWriter(colNr));
     		IStringWriter* stringWriter = stringWriterP.get();
        fdsWriteFactorVec_v7(myfile, intP, stringWriter, nrOfRows, compress, stringWriter->Encoding(), annotation, hasAnnotation);
        break;
      }

      case FstColumnType::INT_32:
      {
        colTypes[colNr] = 8;
        int* intP = fstTable.GetIntWriter(colNr);
        fdsWriteIntVec_v8(myfile, intP, nrOfRows, compress, annotation, hasAnnotation);
        break;
      }

      case FstColumnType::DOUBLE_64:
      {
        colTypes[colNr] = 9;
        double* doubleP = fstTable.GetDoubleWriter(colNr);
        fdsWriteRealVec_v9(myfile, doubleP, nrOfRows, compress, annotation, hasAnnotation);
        break;
      }

      case FstColumnType::BOOL_2:
      {
        colTypes[colNr] = 10;
        int* intP = fstTable.GetLogicalWriter(colNr);
        fdsWriteLogicalVec_v10(myfile, intP, nrOfRows, compress, annotation, hasAnnotation);
        break;
      }

      case FstColumnType::INT_64:
      {
        colTypes[colNr] = 11;
        long long* intP = fstTable.GetInt64Writer(colNr);
        fdsWriteInt64Vec_v11(myfile, intP, nrOfRows, compress, annotation, hasAnnotation);
        break;
      }

	  case FstColumnType::BYTE:
	  {
		  colTypes[colNr] = 12;
		  char* byteP = fstTable.GetByteWriter(colNr);
		  fdsWriteByteVec_v12(myfile, byteP, nrOfRows, compress, annotation, hasAnnotation);
		  break;
	  }

    default:
        myfile.close();
        throw(runtime_error("Unknown type found in column."));
    }
  }

  // update chunk position data
  *p_chunkPos = positionData[0] - 8 * nrOfCols - DATA_INDEX_SIZE;

  // Calculate header hashes
  *p_chunksetHash = XXH64(&metaDataWriteBlock[tableHeaderSize + keyIndexHeaderSize + 8], chunksetHeaderSize - 8, FST_HASH_SEED);
  *p_chunkIndexHash = XXH64(&chunkIndex[8], CHUNK_INDEX_SIZE - 8, FST_HASH_SEED);

  myfile.seekp(0);
  myfile.write(metaDataWriteBlock, metaDataSize);  // table header

  *p_chunkDataHash = XXH64(&chunkIndex[CHUNK_INDEX_SIZE + 8], chunkIndexSize - (CHUNK_INDEX_SIZE + 8), FST_HASH_SEED);

  myfile.seekp(*p_chunkPos - CHUNK_INDEX_SIZE);
  myfile.write(chunkIndex, chunkIndexSize);  // vertical chunkset index and positiondata

  // Check file status only here for performance.
  // Any error that was generated earlier will result in a fail here.
  if (myfile.fail())
  {
	  myfile.close();

	  throw(runtime_error("There was an error during the write operation, fst file might be corrupted. Please check available disk space and access rights."));
  }

  myfile.close();
}


void FstStore::fstMeta(IColumnFactory* columnFactory)
{
  // fst file stream using a stack buffer
  ifstream myfile;
  myfile.open(fstFile.c_str(), ios::in | ios::binary);

  if (myfile.fail())
  {
    myfile.close();
    throw(runtime_error(FSTERROR_ERROR_OPENING_FILE));
  }

  // Read variables from fst file header and check header hash
  tableVersionMax = ReadHeader(myfile, keyLength, nrOfCols);

  unsigned long long keyIndexHeaderSize = 0;

  if (keyLength != 0)
  {
    // size of key index vector and hash and a possible allignment correction
    keyIndexHeaderSize = (keyLength % 2) * 4 + 4 * (keyLength + 2);
  }

  const unsigned long long chunksetHeaderSize = CHUNKSET_HEADER_SIZE + 8 * nrOfCols;
  const unsigned long long colNamesHeaderSize = 24;
  const unsigned long long metaSize = keyIndexHeaderSize + chunksetHeaderSize + colNamesHeaderSize;

  // Read format headers
  metaDataBlockP = std::unique_ptr<char[]>(new char[metaSize]);
  metaDataBlock = metaDataBlockP.get();

  myfile.read(metaDataBlock, metaSize);

  if (keyLength != 0)
  {
    keyColPos = reinterpret_cast<int*>(&metaDataBlock[8]);  // equals nullptr if there are no keys

    unsigned long long* p_keyIndexHash = reinterpret_cast<unsigned long long*>(metaDataBlock);
    const unsigned long long hHash = XXH64(&metaDataBlock[8], keyIndexHeaderSize - 8, FST_HASH_SEED);

    if (*p_keyIndexHash != hHash)
    {
      myfile.close();
      throw(runtime_error(FSTERROR_DAMAGED_HEADER));
    }
  }

  // Chunkset header [node C, free leaf of A or other chunkset header] [size: 80 + 8 * nrOfCols]

  unsigned long long* p_chunksetHash        = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize]);
  //unsigned int* p_chunksetHeaderVersion   = reinterpret_cast<unsigned int*>(&metaDataBlock[keyIndexHeaderSize + 8]);
  //int* p_chunksetFlags                    = reinterpret_cast<int*>(&metaDataBlock[keyIndexHeaderSize + 12]);
  //unsigned long long* p_freeBytes2        = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 16]);
  //unsigned long long* p_freeBytes3        = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 24]);
  //unsigned long long* p_colNamesPos       = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 32]);

  //unsigned long long* p_nextHorzChunkSet  = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 40]);
  //unsigned long long* p_primChunksetIndex = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 48]);
  //unsigned long long* p_secChunksetIndex  = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 56]);
  p_nrOfRows                              = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 64]);
  //int* p_nrOfChunksetCols                 = reinterpret_cast<int*>(&metaDataBlock[keyIndexHeaderSize + 72]);

  colAttributeTypes                       = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + 80]);
  colTypes                                = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + 80 + 2 * nrOfCols]);
  colBaseTypes                            = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + 80 + 4 * nrOfCols]);
  colScales                               = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + 80 + 6 * nrOfCols]);

  const unsigned long long chunksetHash = XXH64(&metaDataBlock[keyIndexHeaderSize + 8], chunksetHeaderSize - 8, FST_HASH_SEED);
  if (*p_chunksetHash != chunksetHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_HEADER));
  }

  // Column names header

  const unsigned long long offset = keyIndexHeaderSize + chunksetHeaderSize;
  unsigned long long* p_colNamesHash = reinterpret_cast<unsigned long long*>(&metaDataBlock[offset]);
  //unsigned int* p_colNamesVersion = reinterpret_cast<unsigned int*>(&metaDataBlock[offset + 8]);
  //int* p_colNamesFlags = reinterpret_cast<int*>(&metaDataBlock[offset + 12]);
  //unsigned long long* p_freeBytes4 = reinterpret_cast<unsigned long long*>(&metaDataBlock[16]);

  const unsigned long long colNamesHash = XXH64(&metaDataBlock[offset + 8], colNamesHeaderSize - 8, FST_HASH_SEED);

  if (*p_colNamesHash != colNamesHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_HEADER));
  }

  // Read column names
  const unsigned long long colNamesOffset = metaSize + TABLE_META_SIZE;

  blockReaderP = std::unique_ptr<IStringColumn>(columnFactory->CreateStringColumn(nrOfCols, FstColumnAttribute::NONE));
  blockReader = blockReaderP.get();

  fdsReadCharVec_v6(myfile, blockReader, colNamesOffset, 0, static_cast<unsigned int>(nrOfCols), static_cast<unsigned int>(nrOfCols));

  // cleanup
  myfile.close();
}


void FstStore::fstRead(IFstTable &tableReader, IStringArray* columnSelection, const long long startRow, const long long endRow,
  IColumnFactory* columnFactory, vector<int> &keyIndex, IStringArray* selectedCols)
{
  // fst file stream using a stack buffer
  ifstream myfile;
  myfile.open(fstFile.c_str(), ios::in | ios::binary);  // only nead an input stream reader

  if (myfile.fail())
  {
    myfile.close();
    throw(runtime_error(FSTERROR_ERROR_OPENING_FILE));
  }

  int keyLength;
  tableVersionMax = ReadHeader(myfile, keyLength, nrOfCols);

  unsigned long long keyIndexHeaderSize = 0;

  if (keyLength != 0)
  {
    // size of key index vector and hash and a possible allignment correction
    keyIndexHeaderSize = (keyLength % 2) * 4 + 4 * (keyLength + 2);
  }

  const unsigned long long chunksetHeaderSize = CHUNKSET_HEADER_SIZE + 8 * nrOfCols;
  const unsigned long long colNamesHeaderSize = 24;
  const unsigned long long metaSize = keyIndexHeaderSize + chunksetHeaderSize + colNamesHeaderSize;

  // Read format headers
  metaDataBlockP = std::unique_ptr<char[]>(new char[metaSize]);
  metaDataBlock = metaDataBlockP.get();

  myfile.read(metaDataBlock, metaSize);

  int* keyColPos = reinterpret_cast<int*>(&metaDataBlock[8]);  // TODO: why not unsigned ?

  if (keyLength != 0)
  {
    unsigned long long* p_keyIndexHash = reinterpret_cast<unsigned long long*>(metaDataBlock);
    const unsigned long long hHash = XXH64(&metaDataBlock[8], keyIndexHeaderSize - 8, FST_HASH_SEED);

    if (*p_keyIndexHash != hHash)
    {
      myfile.close();
      throw(runtime_error(FSTERROR_DAMAGED_HEADER));
    }
  }

  // Chunkset header [node C, free leaf of A or other chunkset header] [size: 80 + 8 * nrOfCols]
  
  unsigned long long* p_chunksetHash      = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize]);
  //unsigned int* p_chunksetHeaderVersion   = reinterpret_cast<unsigned int*>(&metaDataBlock[keyIndexHeaderSize + 8]);
  //int* p_chunksetFlags                    = reinterpret_cast<int*>(&metaDataBlock[keyIndexHeaderSize + 12]);
  //unsigned long long* p_freeBytes2        = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 16]);
  //unsigned long long* p_freeBytes3        = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 24]);
  //unsigned long long* p_colNamesPos       = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 32]);

  //unsigned long long* p_nextHorzChunkSet  = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 40]);
  //unsigned long long* p_primChunksetIndex = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 48]);
  //unsigned long long* p_secChunksetIndex  = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 56]);
  //unsigned long long* p_nrOfRows          = reinterpret_cast<unsigned long long*>(&metaDataBlock[keyIndexHeaderSize + 64]);
  //int* p_nrOfChunksetCols                 = reinterpret_cast<int*>(&metaDataBlock[keyIndexHeaderSize + 72]);
  //int* p_freeBytes4                       = reinterpret_cast<int*>(&metaDataBlock[offset + 76]);


  colAttributeTypes                       = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + CHUNKSET_HEADER_SIZE]);
  colTypes                                = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + CHUNKSET_HEADER_SIZE + 2 * nrOfCols]);
  colBaseTypes                            = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + CHUNKSET_HEADER_SIZE + 4 * nrOfCols]);
  colScales                               = reinterpret_cast<unsigned short int*>(&metaDataBlock[keyIndexHeaderSize + CHUNKSET_HEADER_SIZE + 6 * nrOfCols]);

  const unsigned long long chunksetHash = XXH64(&metaDataBlock[keyIndexHeaderSize + 8], chunksetHeaderSize - 8, FST_HASH_SEED);
  if (*p_chunksetHash != chunksetHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_HEADER));
  }

  // Column names header

  const unsigned long long offset          = keyIndexHeaderSize + chunksetHeaderSize;
  unsigned long long* p_colNamesHash = reinterpret_cast<unsigned long long*>(&metaDataBlock[offset]);
  //unsigned int* p_colNamesVersion    = reinterpret_cast<unsigned int*>(&metaDataBlock[offset + 8]);
  //int* p_colNamesFlags               = reinterpret_cast<int*>(&metaDataBlock[offset + 12]);
  //unsigned long long* p_freeBytes5   = reinterpret_cast<unsigned long long*>(&metaDataBlock[offset + 16]);

  const unsigned long long colNamesHash = XXH64(&metaDataBlock[offset + 8], colNamesHeaderSize - 8, FST_HASH_SEED);
  if (*p_colNamesHash != colNamesHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_HEADER));
  }

  // Column names

  const unsigned long long colNamesOffset = metaSize + TABLE_META_SIZE;

  blockReaderP = std::unique_ptr<IStringColumn>(columnFactory->CreateStringColumn(nrOfCols, FstColumnAttribute::NONE));
  blockReader = blockReaderP.get();

  fdsReadCharVec_v6(myfile, blockReader, colNamesOffset, 0, static_cast<unsigned int>(nrOfCols), static_cast<unsigned int>(nrOfCols));

  // Size of chunkset index header plus data chunk header
  const unsigned long long chunkIndexSize = CHUNK_INDEX_SIZE + DATA_INDEX_SIZE + 8 * nrOfCols;
  char* chunkIndex = new char[chunkIndexSize];
  std::unique_ptr<char[]> chunkIndexPtr = std::unique_ptr<char[]>(chunkIndex);

  myfile.read(chunkIndex, chunkIndexSize);

  // Chunk index [node D, leaf of C] [size: 96]

  unsigned long long* p_chunkIndexHash = reinterpret_cast<unsigned long long*>(chunkIndex);
  //unsigned int* p_chunkIndexVersion    = reinterpret_cast<unsigned int*>(&chunkIndex[8]);
  //int* p_chunkIndexFlags               = reinterpret_cast<int*>(&chunkIndex[12]);
  //unsigned long long* p_freeBytes6     = reinterpret_cast<unsigned long long*>(&chunkIndex[16]);
  //unsigned short int* p_nrOfChunkSlots = reinterpret_cast<unsigned short int*>(&chunkIndex[24]);
  //unsigned short int* p_freeBytes7     = reinterpret_cast<unsigned short int*>(&chunkIndex[26]);
  //unsigned long long* p_chunkPos       = reinterpret_cast<unsigned long long*>(&chunkIndex[32]);
  unsigned long long* p_chunkRows      = reinterpret_cast<unsigned long long*>(&chunkIndex[64]);

  // Chunk data header [node E, leaf of D] [size: 24 + 8 * nrOfCols]

  unsigned long long* p_chunkDataHash  = reinterpret_cast<unsigned long long*>(&chunkIndex[96]);
  //unsigned int* p_chunkDataVersion     = reinterpret_cast<unsigned int*>(&chunkIndex[104]);
  //int* p_chunkDataFlags                = reinterpret_cast<int*>(&chunkIndex[108]);
  //unsigned long long* p_freeBytes8     = reinterpret_cast<unsigned long long*>(&chunkIndex[112]);
  unsigned long long* positionData     = reinterpret_cast<unsigned long long*>(&chunkIndex[120]);  // column position index


  // Check chunk hashes

  const unsigned long long chunkIndexHash = XXH64(&chunkIndex[8], CHUNK_INDEX_SIZE - 8, FST_HASH_SEED);

  if (*p_chunkIndexHash != chunkIndexHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_CHUNKINDEX));
  }

  const unsigned long long chunkDataHash = XXH64(&chunkIndex[CHUNK_INDEX_SIZE + 8], chunkIndexSize - (CHUNK_INDEX_SIZE + 8), FST_HASH_SEED);

  if (*p_chunkDataHash != chunkDataHash)
  {
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_CHUNKINDEX));
  }


  // Read block positions
  unsigned long long* blockPos = positionData;


  // Determine column selection
  std::unique_ptr<int[]> colIndexP;
  int *colIndex = nullptr;

  int nrOfSelect;

  if (columnSelection == nullptr)
  {
    colIndexP = std::unique_ptr<int[]>(new int[nrOfCols]);
    colIndex = colIndexP.get();

    for (int colNr = 0; colNr < nrOfCols; ++colNr)
    {
      colIndex[colNr] = colNr;
    }
    nrOfSelect = nrOfCols;
  }
  else  // determine column numbers of column names
  {
    nrOfSelect = columnSelection->Length();

    colIndexP = std::unique_ptr<int[]>(new int[nrOfSelect]);
    colIndex = colIndexP.get();

    for (int colSel = 0; colSel < nrOfSelect; ++colSel)
    {
      int equal = -1;
      const char* str1 = columnSelection->GetElement(colSel);

      for (int colNr = 0; colNr < nrOfCols; ++colNr)
      {
        const char* str2 = blockReader->GetElement(colNr);
        if (strcmp(str1, str2) == 0)
        {
          equal = colNr;
          break;
        }
      }

      if (equal == -1)
      {
        myfile.close();
        throw(runtime_error("Selected column not found."));
      }

      colIndex[colSel] = equal;
    }
  }


  // Check range of selected rows
  const long long firstRow = startRow - 1;
  const unsigned long long nrOfRows = *p_chunkRows;  // TODO: check for row numbers > INT_MAX !!!

  if (firstRow >= static_cast<long long>(nrOfRows) || firstRow < 0)
  {
    myfile.close();

    if (firstRow < 0)
    {
      throw(runtime_error("Parameter fromRow should have a positive value."));
    }

    throw(runtime_error("Row selection is out of range."));
  }

  long long length = nrOfRows - firstRow;


  // Determine vector length
  if (endRow != -1)
  {
    if (static_cast<long long>(endRow) <= firstRow)
    {
      myfile.close();
      throw(runtime_error("Incorrect row range specified."));
    }

    length = min(endRow - firstRow, static_cast<long long>(nrOfRows) - firstRow);
  }

  tableReader.InitTable(nrOfSelect, length);

  for (int colSel = 0; colSel < nrOfSelect; ++colSel)
  {
    const int colNr = colIndex[colSel];

    if (colNr < 0 || colNr >= nrOfCols)
    {
      myfile.close();
      throw(runtime_error("Column selection is out of range."));
    }

    const unsigned long long pos = blockPos[colNr];
    const short int scale = colScales[colNr];

    switch (colTypes[colNr])
    {
    // Character vector
      case 6:
      {
        std::unique_ptr<IStringColumn> stringColumnP(columnFactory->CreateStringColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr])));
        IStringColumn* stringColumn = stringColumnP.get();
        fdsReadCharVec_v6(myfile, stringColumn, pos, firstRow, length, nrOfRows);
        tableReader.SetStringColumn(stringColumn, colSel);
        break;
      }

      // Integer vector
      case 8:
      {
        std::unique_ptr<IIntegerColumn> integerColumnP(columnFactory->CreateIntegerColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]), scale));
        IIntegerColumn* integerColumn = integerColumnP.get();

        std::string annotation = "";
        bool hasAnnotation;
        fdsReadIntVec_v8(myfile, integerColumn->Data(), pos, firstRow, length, nrOfRows, annotation, hasAnnotation);

        if (hasAnnotation)
        {
          tableReader.SetIntegerColumn(integerColumn, colSel, annotation);
          break;
        }

        tableReader.SetIntegerColumn(integerColumn, colSel);
        break;
      }

      // Double vector
      case 9:
      {
        std::unique_ptr<IDoubleColumn> doubleColumnP(columnFactory->CreateDoubleColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]), scale));
        IDoubleColumn* doubleColumn = doubleColumnP.get();

        std::string annotation = "";
        bool hasAnnotation;
        fdsReadRealVec_v9(myfile, doubleColumn->Data(), pos, firstRow, length, nrOfRows, annotation, hasAnnotation);

        if (hasAnnotation)
        {
          tableReader.SetDoubleColumn(doubleColumn, colSel, annotation);
          break;
        }

        tableReader.SetDoubleColumn(doubleColumn, colSel);
        break;
      }

      // Logical vector
      case 10:
      {
        std::unique_ptr<ILogicalColumn> logicalColumnP(columnFactory->CreateLogicalColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr])));
        ILogicalColumn* logicalColumn = logicalColumnP.get();
        fdsReadLogicalVec_v10(myfile, logicalColumn->Data(), pos, firstRow, length, nrOfRows);
        tableReader.SetLogicalColumn(logicalColumn, colSel);
        break;
      }

      // Factor vector
      case 7:
      {
        std::unique_ptr<IFactorColumn> factorColumnP(columnFactory->CreateFactorColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr])));
        IFactorColumn* factorColumn = factorColumnP.get();
        fdsReadFactorVec_v7(myfile, factorColumn->Levels(), factorColumn->LevelData(), pos, firstRow, length, nrOfRows);
        tableReader.SetFactorColumn(factorColumn, colSel);
        break;
      }

	  // integer64 vector
	  case 11:
	  {
      std::unique_ptr<IInt64Column> int64ColumP(columnFactory->CreateInt64Column(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]), scale));
	    IInt64Column* int64Column = int64ColumP.get();
      fdsReadInt64Vec_v11(myfile, int64Column->Data(), pos, firstRow, length, nrOfRows);
	    tableReader.SetInt64Column(int64Column, colSel);
	    break;
	  }

	  // byte vector
	  case 12:
	  {
      std::unique_ptr<IByteColumn> byteColumnP(columnFactory->CreateByteColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr])));
      IByteColumn* byteColumn = byteColumnP.get();
		  fdsReadByteVec_v12(myfile, byteColumn->Data(), pos, firstRow, length, nrOfRows);
		  tableReader.SetByteColumn(byteColumn, colSel);
		  break;
	  }

    default:
      myfile.close();
      throw(runtime_error("Unknown type found in column."));
    }
  }

  myfile.close();

  // Key index
  SetKeyIndex(keyIndex, keyLength, nrOfSelect, keyColPos, colIndex);

  selectedCols->AllocateArray(nrOfSelect);
  selectedCols->SetEncoding(blockReader->GetEncoding());

  for (int i = 0; i < nrOfSelect; ++i)
  {
    selectedCols->SetElement(i, blockReader->GetElement(colIndex[i]));
  }
}
