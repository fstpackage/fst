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


#include <iostream>
#include <fstream>
#include <stdexcept>
#include <cstring>
#include <algorithm>

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

#include <ZSTD/common/xxhash.h>


using namespace std;



// Table metadata
//
//  NR OF BYTES            | TYPE               | VARIABLE NAME
//
//  8                      | unsigned long long | FST_FILE_ID
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | header hash
//  4                      | int                | keyLength
//  4                      | int                | nrOfCols  (duplicate for fast access)
//
// Chunk indirections (for multi-chunk fst files)
//
//  8                      | unsigned long long | nextHorzChunkSet  (reference to chunk with column metadata)
//  8                      | unsigned long long | empty, for future expansions (key table?)
//
// Vertical chunk metadata
//  8                      | unsigned long long | nrOfRows
//  4 * keyLength          | int                | keyColPos
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | nrOfCols
//  2 * nrOfCols           | unsigned short int | colAttributesType
//  2 * nrOfCols           | unsigned short int | colTypes
//  2 * nrOfCols           | unsigned short int | colBaseTypes
//  2 * nrOfCols           | unsigned short int | colScales          // use for pico, nano, micro, milli, kilo, mega, giga, tera etc.
//
// Column names
//
//  x                      | char               | colNames
//
// Data chunkset index
//
//  8 * 8 (index rows)     | unsigned long long | chunkPos
//  8 * 8 (index rows)     | unsigned long long | chunkRows
//  8                      | unsigned long long | nrOfChunksPerIndexRow
//  8                      | unsigned long long | nrOfChunks
//
// Data chunk columnar position data
//
//  8 * nrOfCols           | unsigned long long | positionData
//
//

FstStore::FstStore(std::string fstFile)
{
  this->fstFile = fstFile;
  metaDataBlock = nullptr;
  blockReader = nullptr;
}


// Read header information
inline unsigned int ReadHeader(ifstream &myfile, unsigned int &metaHash, int &keyLength, int &nrOfColsFirstChunk)
{
  // Get meta-information for table
  char tableMeta[TABLE_META_SIZE];
  myfile.read(tableMeta, TABLE_META_SIZE);

  if (!myfile)
  {
    myfile.close();
    throw(runtime_error("Error reading file header, your fst file is incomplete or damaged."));
  }


  unsigned long long* p_fstFileID = reinterpret_cast<unsigned long long*>(tableMeta);
  unsigned int* p_table_version   = reinterpret_cast<unsigned int*>(&tableMeta[8]);

  unsigned int* p_metaHash        = reinterpret_cast<unsigned int*>(&tableMeta[12]);
  int* p_keyLength                = reinterpret_cast<int*>(&tableMeta[16]);
  int* p_nrOfColsFirstChunk       = reinterpret_cast<int*>(&tableMeta[20]);

  metaHash           = *p_metaHash;
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
    myfile.close();
    throw(runtime_error("Incompatible fst file: file was created by a newer version of the fst package."));
  }

  return *p_table_version;
}


inline void SetKeyIndex(vector<int> &keyIndex, int keyLength, int nrOfSelect, int* keyColPos, int* colIndex)
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


void FstStore::fstWrite(IFstTable &fstTable, int compress) const
{
  // Meta on dataset
  int nrOfCols =  fstTable.NrOfColumns();
  int keyLength = fstTable.NrOfKeys();

  if (nrOfCols == 0)
  {
    throw(runtime_error("Your dataset needs at least one column."));
  }


  // Table meta information
  unsigned long long metaDataSize        = 56 + 4 * keyLength + 8 * nrOfCols;  // see index above
  char* metaDataBlock                    = new char[metaDataSize];

  unsigned long long* fstFileID          = (unsigned long long*) metaDataBlock;
  unsigned int* p_table_version          = (unsigned int*) &metaDataBlock[8];
  unsigned int* headerHash               = (unsigned int*) &metaDataBlock[12];
  int* p_keyLength                       = (int*) &metaDataBlock[16];
  int* p_nrOfColsFirstChunk              = (int*) &metaDataBlock[20];

  unsigned long long* p_nextHorzChunkSet = (unsigned long long*) &metaDataBlock[24];
  unsigned long long* p_emptySlot        = (unsigned long long*) &metaDataBlock[32];
  unsigned long long* p_nrOfRows         = (unsigned long long*) &metaDataBlock[40];

  int* keyColPos                         = (int*) &metaDataBlock[48];

  unsigned int offset = 48 + 4 * keyLength;

  unsigned int* p_version                = (unsigned int*) &metaDataBlock[offset];
  int* p_nrOfCols                        = (int*) &metaDataBlock[offset + 4];
  unsigned short int* colAttributeTypes  = (unsigned short int*) &metaDataBlock[offset + 8];
  unsigned short int* colTypes           = (unsigned short int*) &metaDataBlock[offset + 8 + 2 * nrOfCols];
  unsigned short int* colBaseTypes       = (unsigned short int*) &metaDataBlock[offset + 8 + 4 * nrOfCols];
  short int* colScales                   = (short int*) &metaDataBlock[offset + 8 + 6 * nrOfCols];          // column's order of magnitude

  // Get key column positions
  fstTable.GetKeyColumns(keyColPos);

  *fstFileID            = FST_FILE_ID;
  *p_table_version      = FST_VERSION;
  *p_keyLength          = keyLength;
  *p_nrOfColsFirstChunk = nrOfCols;

  *p_nextHorzChunkSet   = 0;
  *p_emptySlot          = 0;            // reserved for future use
  *p_version            = FST_VERSION;
  *p_nrOfCols           = nrOfCols;


  // data.frame code here for stability!

  int nrOfRows = fstTable.NrOfRows();
  *p_nrOfRows = nrOfRows;


  if (nrOfRows == 0)
  {
    delete[] metaDataBlock;
    throw(runtime_error("The dataset contains no data."));
  }


  // Create file, set fast local buffer and open
  ofstream myfile;
  myfile.open(fstFile.c_str(), ios::out | ios::binary);

  if (myfile.fail())
  {
    delete[] metaDataBlock;
    myfile.close();
    throw(runtime_error("There was an error creating the file. Please check for a correct filename."));
  }


  // Write table meta information
  myfile.write((char*)(metaDataBlock), metaDataSize);  // table meta data

  // Serialize column names
  IStringWriter* blockRunner = fstTable.GetColNameWriter();
  fdsWriteCharVec_v6(myfile, blockRunner, 0, StringEncoding::NATIVE);   // column names
  delete blockRunner;

  // Vertical chunkset index or index of index
  char* chunkIndex = new char[CHUNK_INDEX_SIZE + 8 * nrOfCols];

  unsigned long long* chunkPos                = (unsigned long long*) chunkIndex;
  unsigned long long* chunkRows               = (unsigned long long*) &chunkIndex[64];
  unsigned long long* p_nrOfChunksPerIndexRow = (unsigned long long*) &chunkIndex[128];
  unsigned long long* p_nrOfChunks            = (unsigned long long*) &chunkIndex[136];
  unsigned long long *positionData            = (unsigned long long*) &chunkIndex[144];  // column position index


  *p_nrOfChunksPerIndexRow = 1;
  *p_nrOfChunks            = 1;  // set to 0 if all reserved slots are used
  *chunkRows               = (unsigned long long) nrOfRows;


  // Row and column meta data
  myfile.write((char*)(chunkIndex), CHUNK_INDEX_SIZE + 8 * nrOfCols);   // file positions of column data

  // column data
  for (int colNr = 0; colNr < nrOfCols; ++colNr)
  {
    positionData[colNr] = myfile.tellp();  // current location
  	FstColumnAttribute colAttribute;
  	std::string annotation = "";
    short int scale = 0;

  	// get type and add annotation
    FstColumnType colType = fstTable.ColumnType(colNr, colAttribute, scale, annotation);

    colBaseTypes[colNr] = static_cast<unsigned short int>(colType);
  	colAttributeTypes[colNr] = static_cast<unsigned short int>(colAttribute);
    colScales[colNr] = scale;

    switch (colType)
    {
      case FstColumnType::CHARACTER:
      {
        colTypes[colNr] = 6;
     		IStringWriter* stringWriter = fstTable.GetStringWriter(colNr);
        fdsWriteCharVec_v6(myfile, stringWriter, compress, stringWriter->Encoding());   // column names
     		delete stringWriter;
        break;
      }

      case FstColumnType::FACTOR:
      {
        colTypes[colNr] = 7;
        int* intP = fstTable.GetIntWriter(colNr);  // level values pointer
     		IStringWriter* stringWriter = fstTable.GetLevelWriter(colNr);
        fdsWriteFactorVec_v7(myfile, intP, stringWriter, nrOfRows, compress, stringWriter->Encoding(), annotation);
	      delete stringWriter;
        break;
      }

      case FstColumnType::INT_32:
      {
        colTypes[colNr] = 8;
        int* intP = fstTable.GetIntWriter(colNr);
        fdsWriteIntVec_v8(myfile, intP, nrOfRows, compress, annotation);
        break;
      }

      case FstColumnType::DOUBLE_64:
      {
        colTypes[colNr] = 9;
        double* doubleP = fstTable.GetDoubleWriter(colNr);
        fdsWriteRealVec_v9(myfile, doubleP, nrOfRows, compress, annotation);
        break;
      }

      case FstColumnType::BOOL_2:
      {
        colTypes[colNr] = 10;
        int* intP = fstTable.GetLogicalWriter(colNr);
        fdsWriteLogicalVec_v10(myfile, intP, nrOfRows, compress, annotation);
        break;
      }

      case FstColumnType::INT_64:
      {
        colTypes[colNr] = 11;
        long long* intP = fstTable.GetInt64Writer(colNr);
        fdsWriteInt64Vec_v11(myfile, (long long*) intP, nrOfRows, compress, annotation);
        break;
      }

	  case FstColumnType::BYTE:
	  {
		  colTypes[colNr] = 12;
		  char* byteP = fstTable.GetByteWriter(colNr);
		  fdsWriteByteVec_v12(myfile, byteP, nrOfRows, compress, annotation);
		  break;
	  }

    default:
        delete[] metaDataBlock;
        delete[] chunkIndex;
        myfile.close();
        throw(runtime_error("Unknown type found in column."));
    }
  }

  // update chunk position data
  *chunkPos = positionData[0] - 8 * nrOfCols;

  // Calculate header hash
  unsigned long long hHash = XXH64(&metaDataBlock[24], metaDataSize - 24, FST_HASH_SEED);
  *headerHash = static_cast<unsigned long long>((hHash >> 32) & 0xffffffff);

  myfile.seekp(0);
  myfile.write((char*)(metaDataBlock), metaDataSize);  // table header

  myfile.seekp(*chunkPos - CHUNK_INDEX_SIZE);
  myfile.write((char*)(chunkIndex), CHUNK_INDEX_SIZE + 8 * nrOfCols);  // vertical chunkset index and positiondata

  // cleanup
  delete[] metaDataBlock;
  delete[] chunkIndex;

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
    throw(runtime_error("There was an error opening the fst file, please check for a correct path."));
  }

  // Read variables from fst file header
  version = ReadHeader(myfile, metaHash, keyLength, nrOfColsFirstChunk);

  // We may be looking at a fst v0.7.2 file format
  if (version == 0)
  {
    // Close and reopen (slow: fst file should be resaved to avoid)
    myfile.close();
	throw(runtime_error(FSTERROR_NON_FST_FILE));
  }

  // Continue reading table metadata
  int metaSize = 32 + 4 * keyLength + 8 * nrOfColsFirstChunk;
  metaDataBlock = new char[metaSize];
  myfile.read(metaDataBlock, metaSize);

  unsigned int tmpOffset = 4 * keyLength;

  // unsigned long long* p_nextHorzChunkSet = (unsigned long long*) metaDataBlock;
  // unsigned long long* p_nextVertChunkSet = (unsigned long long*) &metaDataBlock[8];
  p_nrOfRows                                = (unsigned long long*) &metaDataBlock[16];

  keyColPos                                 = (int*) &metaDataBlock[24];

  // unsigned int* p_version                = (unsigned int*) &metaDataBlock[tmpOffset + 24];
  int* p_nrOfCols                           = (int*) &metaDataBlock[tmpOffset + 28];
  colAttributeTypes                         = (unsigned short int*) &metaDataBlock[tmpOffset + 32];
  colTypes                                  = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 2 * nrOfColsFirstChunk];
  colBaseTypes                              = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 4 * nrOfColsFirstChunk];
  colScales                                 = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 6 * nrOfColsFirstChunk];


  nrOfCols = *p_nrOfCols;

  // Read column names
  unsigned long long offset = metaSize + TABLE_META_SIZE;

  blockReader = columnFactory->CreateStringColumn(nrOfCols, FstColumnAttribute::NONE);
  fdsReadCharVec_v6(myfile, blockReader, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);

  // cleanup
  myfile.close();
}


void FstStore::fstRead(IFstTable &tableReader, IStringArray* columnSelection, int startRow, int endRow, IColumnFactory* columnFactory, vector<int> &keyIndex, IStringArray* selectedCols)
{
  // fst file stream using a stack buffer
  ifstream myfile;
  myfile.open(fstFile.c_str(), ios::in | ios::binary);  // only nead an input stream reader

  if (myfile.fail())
  {
    myfile.close();
    throw(runtime_error(FSTERROR_ERROR_OPENING_FILE));
  }

  unsigned int metaHash;
  int keyLength, nrOfColsFirstChunk;
  version = ReadHeader(myfile, metaHash, keyLength, nrOfColsFirstChunk);

  // No magic marker for fst format found (v0.7.2 file format?)
  if (version == 0)
  {
    // Close and reopen (slow: fst file should be resaved to avoid this overhead)
	  myfile.close();
	  throw(runtime_error(FSTERROR_NON_FST_FILE));
  }


  // Continue reading table metadata
  int metaSize = 32 + 4 * keyLength + 8 * nrOfColsFirstChunk;
  char* metaDataBlock = new char[metaSize];
  myfile.read(metaDataBlock, metaSize);

  // Verify header hash
  unsigned long long hHash = XXH64(metaDataBlock, metaSize, FST_HASH_SEED);
  unsigned int headerHash = (hHash >> 32) & 0xffffffff;

  if (headerHash != metaHash)
  {
    delete[] metaDataBlock;
    myfile.close();
    throw(runtime_error(FSTERROR_DAMAGED_HEADER));
  }

  unsigned int tmpOffset = 4 * keyLength;

  // unsigned long long* p_nextHorzChunkSet = (unsigned long long*) metaDataBlock;
  // unsigned long long* p_nextVertChunkSet = (unsigned long long*) &metaDataBlock[8];
  // unsigned long long* p_nrOfRows         = (unsigned long long*) &metaDataBlock[16];

  int* keyColPos = reinterpret_cast<int*>(&metaDataBlock[24]);

  // unsigned int* p_version                = (unsigned int*) &metaDataBlock[tmpOffset + 24];
  int* p_nrOfCols                        = reinterpret_cast<int*>(&metaDataBlock[tmpOffset + 28]);
  unsigned short int* colAttributeTypes  = reinterpret_cast<unsigned short int*>(&metaDataBlock[tmpOffset + 32]);
  unsigned short int* colTypes           = reinterpret_cast<unsigned short int*>(&metaDataBlock[tmpOffset + 32 + 2 * nrOfColsFirstChunk]);
  // unsigned short int* colBaseTypes       = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 4 * nrOfColsFirstChunk];
  short int* colScales                   = (short int*) &metaDataBlock[tmpOffset + 32 + 6 * nrOfColsFirstChunk];

  int nrOfCols = *p_nrOfCols;


  // TODO: read table attributes here

  // Read column names
  unsigned long long offset = metaSize + TABLE_META_SIZE;

  // Use a pure C++ charVector implementation here for performance
  IStringColumn* blockReader = columnFactory->CreateStringColumn(nrOfCols, FstColumnAttribute::NONE);
  fdsReadCharVec_v6(myfile, blockReader, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);


  // Vertical chunkset index or index of index
  char chunkIndex[CHUNK_INDEX_SIZE];
  myfile.read(chunkIndex, CHUNK_INDEX_SIZE);

  // unsigned long long* chunkPos                = (unsigned long long*) chunkIndex;
  unsigned long long* chunkRows               = (unsigned long long*) &chunkIndex[64];
  // unsigned long long* p_nrOfChunksPerIndexRow = (unsigned long long*) &chunkIndex[128];
  unsigned long long* p_nrOfChunks            = (unsigned long long*) &chunkIndex[136];

  // Check nrOfChunks
  if (*p_nrOfChunks > 1)
  {
    myfile.close();
    delete[] metaDataBlock;
    delete blockReader;
    throw(runtime_error("Multiple chunk read not implemented yet."));
  }


  // Start reading chunk here. TODO: loop over chunks


  // Read block positions
  unsigned long long* blockPos = new unsigned long long[nrOfCols];
  myfile.read(reinterpret_cast<char*>(blockPos), nrOfCols * 8);  // nrOfCols file positions


  // Determine column selection
  int *colIndex;
  int nrOfSelect;

  if (columnSelection == nullptr)
  {
    colIndex = new int[nrOfCols];

    for (int colNr = 0; colNr < nrOfCols; ++colNr)
    {
      colIndex[colNr] = colNr;
    }
    nrOfSelect = nrOfCols;
  }
  else  // determine column numbers of column names
  {
    nrOfSelect = columnSelection->Length();
    colIndex = new int[nrOfSelect];
    int equal;
    for (int colSel = 0; colSel < nrOfSelect; ++colSel)
    {
      equal = -1;
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
        delete[] metaDataBlock;
        delete[] blockPos;
        delete[] colIndex;
        delete blockReader;
        myfile.close();
        throw(runtime_error("Selected column not found."));
      }

      colIndex[colSel] = equal;
    }
  }


  // Check range of selected rows
  int firstRow = startRow - 1;
  int nrOfRows = *chunkRows;  // TODO: check for row numbers > INT_MAX !!!

  if (firstRow >= nrOfRows || firstRow < 0)
  {
    delete[] metaDataBlock;
    delete[] blockPos;
    delete[] colIndex;
    delete blockReader;
    myfile.close();

    if (firstRow < 0)
    {
      throw(runtime_error("Parameter fromRow should have a positive value."));
    }

    throw(runtime_error("Row selection is out of range."));
  }

  int length = nrOfRows - firstRow;


  // Determine vector length
  if (endRow != -1)
  {
    if (endRow <= firstRow)
    {
      delete[] metaDataBlock;
      delete[] blockPos;
      delete[] colIndex;
      delete blockReader;
      myfile.close();
      throw(runtime_error("Incorrect row range specified."));
    }

    length = min(endRow - firstRow, nrOfRows - firstRow);
  }

  tableReader.InitTable(nrOfSelect, length);

  for (int colSel = 0; colSel < nrOfSelect; ++colSel)
  {
    int colNr = colIndex[colSel];

    if (colNr < 0 || colNr >= nrOfCols)
    {
      delete[] metaDataBlock;
      delete[] blockPos;
      delete[] colIndex;
      delete blockReader;
      myfile.close();
      throw(runtime_error("Column selection is out of range."));
    }

    unsigned long long pos = blockPos[colNr];
    short int scale = colScales[colNr];

    switch (colTypes[colNr])
    {
    // Character vector
      case 6:
      {
        IStringColumn* stringColumn = columnFactory->CreateStringColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]));
        fdsReadCharVec_v6(myfile, stringColumn, pos, firstRow, length, nrOfRows);
        tableReader.SetStringColumn(stringColumn, colSel);
        delete stringColumn;
        break;
      }

      // Integer vector
      case 8:
      {
        IIntegerColumn* integerColumn = columnFactory->CreateIntegerColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]), scale);
        std::string annotation = "";
        fdsReadIntVec_v8(myfile, integerColumn->Data(), pos, firstRow, length, nrOfRows, annotation);
        tableReader.SetIntegerColumn(integerColumn, colSel, annotation);
        delete integerColumn;
        break;
      }

      // Double vector
      case 9:
      {
        IDoubleColumn* doubleColumn = columnFactory->CreateDoubleColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]), scale);
        std::string annotation = "";
        fdsReadRealVec_v9(myfile, doubleColumn->Data(), pos, firstRow, length, nrOfRows, annotation);
        tableReader.SetDoubleColumn(doubleColumn, colSel, annotation);
        delete doubleColumn;
        break;
      }

      // Logical vector
      case 10:
      {
        ILogicalColumn* logicalColumn = columnFactory->CreateLogicalColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]));
        fdsReadLogicalVec_v10(myfile, logicalColumn->Data(), pos, firstRow, length, nrOfRows);
        tableReader.SetLogicalColumn(logicalColumn, colSel);
        delete logicalColumn;
        break;
      }

      // Factor vector
      case 7:
      {
        IFactorColumn* factorColumn = columnFactory->CreateFactorColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]));
        fdsReadFactorVec_v7(myfile, factorColumn->Levels(), factorColumn->LevelData(), pos, firstRow, length, nrOfRows);
        tableReader.SetFactorColumn(factorColumn, colSel);
        delete factorColumn;
        break;
      }

	  // integer64 vector
	  case 11:
	  {
	    IInt64Column* int64Column = columnFactory->CreateInt64Column(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]), scale);
      fdsReadInt64Vec_v11(myfile, int64Column->Data(), pos, firstRow, length, nrOfRows);
	    tableReader.SetInt64Column(int64Column, colSel);
	    delete int64Column;
	    break;
	  }

	  // byte vector
	  case 12:
	  {
		  IByteColumn* byteColumn = columnFactory->CreateByteColumn(length, static_cast<FstColumnAttribute>(colAttributeTypes[colNr]));
		  fdsReadByteVec_v12(myfile, byteColumn->Data(), pos, firstRow, length, nrOfRows);
		  tableReader.SetByteColumn(byteColumn, colSel);
		  delete byteColumn;
		  break;
	  }

    default:
      delete[] metaDataBlock;
      delete[] blockPos;
      delete[] colIndex;
      delete blockReader;
      myfile.close();
      throw(runtime_error("Unknown type found in column."));
    }
  }

  // delete blockReaderStrVec;

  myfile.close();

  // Key index
  SetKeyIndex(keyIndex, keyLength, nrOfSelect, keyColPos, colIndex);

  selectedCols->AllocateArray(nrOfSelect);

  // Only when keys are present in result set, TODO: compute using C++ only !!!
  for (int i = 0; i < nrOfSelect; ++i)
  {
    selectedCols->SetElement(i, blockReader->GetElement(colIndex[i]));
  }

  delete[] metaDataBlock;
  delete[] blockPos;
  delete[] colIndex;
  delete blockReader;
}
