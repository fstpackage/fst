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

#include <Rcpp.h>
#include <iostream>
#include <fstream>

#include <fstdefines.h>
#include <FastStore.h>
#include <FastStore_v1.h>

#include <iblockrunner.h>
#include <iblockwriterfactory.h>
#include <ifsttable.h>

#include <blockrunner_char.h>
#include <blockwriterfactory.h>
#include <fsttable.h>

#include <character_v6.h>
#include <factor_v7.h>
#include <integer_v8.h>
#include <double_v9.h>
#include <logical_v10.h>


using namespace std;
using namespace Rcpp;


inline int FindKey(StringVector colNameList, String item)
{
  int index = -1;
  int found = 0;
  for (Rcpp::StringVector::iterator it = colNameList.begin(); it != colNameList.end(); ++it)
  {
    if (*it == item)
    {
      index = found;
      break;
    }
    ++found;
  }

  return index;
}


// Table metadata
//
//  NR OF BYTES            | TYPE               | VARIABLE NAME
//
//  8                      | unsigned long long | FST_FILE_ID
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | tableClassType
//  4                      | int                | keyLength
//  4                      | int                | nrOfCols  (duplicate for fast access)
//  4 * keyLength          | int                | keyColPos
//
// Column chunkset info
//
//  8                      | unsigned long long | nextHorzChunkSet
//  8                      | unsigned long long | nextVertChunkSet
//  8                      | unsigned long long | nrOfRows
//  4                      | unsigned int       | FST_VERSION
//  4                      | int                | nrOfCols
//  2 * nrOfCols           | unsigned short int | colAttributesType (not implemented yet)
//  2 * nrOfCols           | unsigned short int | colTypes
//  2 * nrOfCols           | unsigned short int | colBaseTypes
//  ?                      | char               | colNames
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

void fstWrite(const char* fileName, IFstTable &fstTable, int compress)
{
  // SEXP keyNames = Rf_getAttrib(table, Rf_mkString("sorted"));

  // Meta on dataset
  int nrOfCols =  fstTable.NrOfColumns();
  int keyLength = fstTable.NrOfKeys();

  if (nrOfCols == 0)
  {
    throw(runtime_error("Your dataset needs at least one column."));
  }


  // Table meta information
  unsigned long long metaDataSize        = 56 + 4 * keyLength + 6 * nrOfCols;  // see index above
  char* metaDataBlock                    = new char[metaDataSize];

  unsigned long long* fstFileID          = (unsigned long long*) metaDataBlock;
  unsigned int* p_table_version          = (unsigned int*) &metaDataBlock[8];
  unsigned int* p_tableClassType         = (unsigned int*) &metaDataBlock[12];
  int* p_keyLength                       = (int*) &metaDataBlock[16];
  int* p_nrOfColsFirstChunk              = (int*) &metaDataBlock[20];
  int* keyColPos                         = (int*) &metaDataBlock[24];

  unsigned int offset = 24 + 4 * keyLength;

  unsigned long long* p_nextHorzChunkSet = (unsigned long long*) &metaDataBlock[offset];
  unsigned long long* p_nextVertChunkSet = (unsigned long long*) &metaDataBlock[offset + 8];
  unsigned long long* p_nrOfRows         = (unsigned long long*) &metaDataBlock[offset + 16];
  unsigned int* p_version                = (unsigned int*) &metaDataBlock[offset + 24];
  int* p_nrOfCols                        = (int*) &metaDataBlock[offset + 28];
  // unsigned short int* colAttributeTypes  = (unsigned short int*) &metaDataBlock[offset + 32];
  unsigned short int* colTypes           = (unsigned short int*) &metaDataBlock[offset + 32 + 2 * nrOfCols];
  unsigned short int* colBaseTypes       = (unsigned short int*) &metaDataBlock[offset + 32 + 4 * nrOfCols];

  // Get key column positions
  fstTable.GetKeyColumns(keyColPos);

  *fstFileID            = FST_FILE_ID;
  *p_table_version      = FST_VERSION;
  *p_tableClassType     = 1;  // default table
  *p_keyLength          = keyLength;
  *p_nrOfColsFirstChunk = nrOfCols;

  *p_nextHorzChunkSet   = 0;
  *p_nextVertChunkSet   = 0;
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
  char ioBuf[4096];
  myfile.rdbuf()->pubsetbuf(ioBuf, 4096);  // workaround for memory leak in ofstream
  myfile.open(fileName, ios::binary);

  if (myfile.fail())
  {
    delete[] metaDataBlock;
    myfile.close();
    throw(runtime_error("There was an error creating the file. Please check for a correct filename."));
  }


  // Write table meta information
  myfile.write((char*)(metaDataBlock), metaDataSize);  // table meta data

  // Serialize column names
  IBlockWriter* blockRunner = fstTable.GetColNameWriter();
  fdsWriteCharVec_v6(myfile, blockRunner, 0);   // column names

  // TODO: Write column attributes here

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
    FstColumnType colType = fstTable.GetColumnType(colNr);
    colBaseTypes[colNr] = (unsigned short int) colType;

    // Store attributes here if any
    // unsigned int attrBlockSize = SerializeObjectAttributes(ofstream &myfile, RObject rObject, serializer);

    switch (colType)
    {
      case FstColumnType::CHARACTER:
      {
        colTypes[colNr] = 6;
        delete blockRunner;
        blockRunner = fstTable.GetCharWriter(colNr);
        fdsWriteCharVec_v6(myfile, blockRunner, compress);   // column names
        break;
      }

      case FstColumnType::FACTOR:
      {
        colTypes[colNr] = 7;
        delete blockRunner;
        int* intP = fstTable.GetIntWriter(colNr);  // level values pointer
        blockRunner = fstTable.GetLevelWriter(colNr);
        fdsWriteFactorVec_v7(myfile, intP, blockRunner, nrOfRows, compress);
        break;
      }

      case FstColumnType::INT_32:
      {
        colTypes[colNr] = 8;
        int* intP = fstTable.GetIntWriter(colNr);
        fdsWriteIntVec_v8(myfile, intP, nrOfRows, compress);
        break;
      }

      case FstColumnType::DOUBLE_64:
      {
        colTypes[colNr] = 9;
        double* doubleP = fstTable.GetDoubleWriter(colNr);
        fdsWriteRealVec_v9(myfile, doubleP, nrOfRows, compress);
        break;
      }

      case FstColumnType::BOOL_32:
      {
        colTypes[colNr] = 10;
        int* intP = fstTable.GetLogicalWriter(colNr);
        fdsWriteLogicalVec_v10(myfile, intP, nrOfRows, compress);
        break;
      }

      default:
        delete[] metaDataBlock;
        delete[] chunkIndex;
        delete blockRunner;
        myfile.close();
        throw(runtime_error("Unknown type found in column."));
    }
  }

  delete blockRunner;

  // update chunk position data
  *chunkPos = positionData[0] - 8 * nrOfCols;

  myfile.seekp(0);
  myfile.write((char*)(metaDataBlock), metaDataSize);  // table header

  myfile.seekp(*chunkPos - CHUNK_INDEX_SIZE);
  myfile.write((char*)(chunkIndex), CHUNK_INDEX_SIZE + 8 * nrOfCols);  // vertical chunkset index and positiondata

  myfile.close();

  // cleanup
  delete[] metaDataBlock;
  delete[] chunkIndex;
}


SEXP fstStore(String fileName, SEXP table, SEXP compression)
{
  if (!Rf_isInteger(compression))
  {
    ::Rf_error("Parameter compression should be an integer value between 0 and 100");
  }

  int compress = *INTEGER(compression);
  if ((compress < 0) | (compress > 100))
  {
    ::Rf_error("Parameter compression should be an integer value between 0 and 100");
  }

  FstTable fstTable(table);

  try
  {
    fstWrite(fileName.get_cstring(), fstTable, compress);
  }
  catch (const std::runtime_error& e)
  {
    ::Rf_error(e.what());
  }

  return table;
}


// Read header information
inline unsigned int ReadHeader(ifstream &myfile, unsigned int &tableClassType, int &keyLength, int &nrOfColsFirstChunk)
{
  // Get meta-information for table
  char tableMeta[TABLE_META_SIZE];
  myfile.read(tableMeta, TABLE_META_SIZE);

  if (!myfile)
  {
    myfile.close();
    ::Rf_error("Error reading file header, your fst file is incomplete or damaged.");
  }


  unsigned long long* p_fstFileID = (unsigned long long*) tableMeta;
  unsigned int* p_table_version   = (unsigned int*) &tableMeta[8];
  // unsigned int* p_tableClassType  = (unsigned int*) &tableMeta[12];
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
    myfile.close();
    ::Rf_error("Incompatible fst file: file was created by a newer version of the fst package.");
  }

  return *p_table_version;
}


List fstMeta(String fileName)
{
  // fst file stream using a stack buffer
  ifstream myfile;
  char ioBuf[4096];
  myfile.rdbuf()->pubsetbuf(ioBuf, 4096);
  myfile.open(fileName.get_cstring(), ios::binary);

  if (myfile.fail())
  {
    myfile.close();
    ::Rf_error("There was an error opening the fst file, please check for a correct path.");
  }

  // Read variables from fst file header
  unsigned int tableClassType;
  int keyLength, nrOfColsFirstChunk;
  unsigned int version = ReadHeader(myfile, tableClassType, keyLength, nrOfColsFirstChunk);

  // We may be looking at a fst v0.7.2 file format
  if (version == 0)
  {
    // Close and reopen (slow: fst file should be resaved to avoid)
    myfile.close();
    return fstMeta_v1(fileName);  // scans further for safety
  }


  // Continue reading table metadata
  int metaSize = 32 + 4 * keyLength + 6 * nrOfColsFirstChunk;
  char* metaDataBlock = new char[metaSize];
  myfile.read(metaDataBlock, metaSize);

  unsigned int tmpOffset = 4 * keyLength;

  int* keyColPos                         = (int*) metaDataBlock;
  // unsigned long long* p_nextHorzChunkSet = (unsigned long long*) &metaDataBlock[tmpOffset];
  // unsigned long long* p_nextVertChunkSet = (unsigned long long*) &metaDataBlock[tmpOffset + 8];
  unsigned long long* p_nrOfRows         = (unsigned long long*) &metaDataBlock[tmpOffset + 16];
  // unsigned int* p_version                = (unsigned int*) &metaDataBlock[tmpOffset + 24];
  int* p_nrOfCols                        = (int*) &metaDataBlock[tmpOffset + 28];
  // unsigned short int* colAttributeTypes  = (unsigned short int*) &metaDataBlock[tmpOffset + 32];
  unsigned short int* colTypes           = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 2 * nrOfColsFirstChunk];
  // unsigned short int* colBaseTypes       = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 4 * nrOfColsFirstChunk];


  int nrOfCols = *p_nrOfCols;


  // Read column names
  unsigned long long offset = metaSize + TABLE_META_SIZE;
  BlockReaderChar* blockReader = new BlockReaderChar();
  fdsReadCharVec_v6(myfile, (IBlockReader*) blockReader, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);
  SEXP colNames = blockReader->StrVector();
  delete blockReader;

  // Convert to integer vector
  IntegerVector colTypeVec(nrOfCols);
  for (int col = 0; col != nrOfCols; ++col)
  {
    colTypeVec[col] = colTypes[col];
  }


  // cleanup
  myfile.close();


  if (keyLength > 0)
  {
    SEXP keyNames;
    PROTECT(keyNames = Rf_allocVector(STRSXP, keyLength));
    for (int i = 0; i < keyLength; ++i)
    {
      SET_STRING_ELT(keyNames, i, STRING_ELT(colNames, keyColPos[i]));
    }

    IntegerVector keyColIndex(keyLength);
    for (int col = 0; col != keyLength; ++col)
    {
      keyColIndex[col] = keyColPos[col];
    }

    UNPROTECT(2);

    return List::create(
      _["nrOfCols"]        = nrOfCols,
      _["nrOfRows"]        = *p_nrOfRows,
      _["fstVersion"]      = version,
      _["colTypeVec"]      = colTypeVec,
      _["keyColIndex"]     = keyColIndex,
      _["keyLength"]       = keyLength,
      _["keyNames"]        = keyNames,
      _["colNames"]        = colNames);
  }

  UNPROTECT(1);

  return List::create(
    _["nrOfCols"]        = nrOfCols,
    _["nrOfRows"]        = *p_nrOfRows,
    _["fstVersion"]      = version,
    _["keyLength"]       = keyLength,
    _["colTypeVec"]      = colTypeVec,
    _["colNames"]        = colNames);
}


SEXP fstRead(SEXP fileName, SEXP columnSelection, SEXP startRow, SEXP endRow)
{
  // fst file stream using a stack buffer
  ifstream myfile;
  char ioBuf[4096];
  myfile.rdbuf()->pubsetbuf(ioBuf, 4096);
  myfile.open(CHAR(STRING_ELT(fileName, 0)), ios::binary);

  if (myfile.fail())
  {
    myfile.close();
    ::Rf_error("There was an error opening the fst file, please check for a correct path.");
  }


  unsigned int tableClassType;
  int keyLength, nrOfColsFirstChunk;
  unsigned int version = ReadHeader(myfile, tableClassType, keyLength, nrOfColsFirstChunk);

  // We may be looking at a fst v0.7.2 file format
  if (version == 0)
  {
    // Close and reopen (slow: fst file should be resaved to avoid this overhead)
    myfile.close();
    return fstRead_v1(fileName, columnSelection, startRow, endRow);
  }


  // Continue reading table metadata
  int metaSize = 32 + 4 * keyLength + 6 * nrOfColsFirstChunk;
  char* metaDataBlock = new char[metaSize];
  myfile.read(metaDataBlock, metaSize);


  int* keyColPos                         = (int*) metaDataBlock;

  unsigned int tmpOffset = 4 * keyLength;

  // unsigned long long* p_nextHorzChunkSet = (unsigned long long*) &metaDataBlock[tmpOffset];
  // unsigned long long* p_nextVertChunkSet = (unsigned long long*) &metaDataBlock[tmpOffset + 8];
  // unsigned long long* p_nrOfRows         = (unsigned long long*) &metaDataBlock[tmpOffset + 16];
  // unsigned int* p_version                = (unsigned int*) &metaDataBlock[tmpOffset + 24];
  int* p_nrOfCols                        = (int*) &metaDataBlock[tmpOffset + 28];
  // unsigned short int* colAttributeTypes  = (unsigned short int*) &metaDataBlock[tmpOffset + 32];
  unsigned short int* colTypes           = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 2 * nrOfColsFirstChunk];
  // unsigned short int* colBaseTypes       = (unsigned short int*) &metaDataBlock[tmpOffset + 32 + 4 * nrOfColsFirstChunk];

  int nrOfCols = *p_nrOfCols;


  // TODO: read table attributes here

  // Read column names
  // SEXP colNames;
  // PROTECT(colNames = Rf_allocVector(STRSXP, nrOfCols));
  unsigned long long offset = metaSize + TABLE_META_SIZE;
  BlockReaderChar* blockReader = new BlockReaderChar();
  fdsReadCharVec_v6(myfile, (IBlockReader*) blockReader, offset, 0, (unsigned int) nrOfCols, (unsigned int) nrOfCols);
  SEXP colNames = blockReader->StrVector();
  delete blockReader;

  // TODO: read column attributes here


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
    ::Rf_error("Multiple chunk read not implemented yet.");
  }


  // Start reading chunk here. TODO: loop over chunks


  // Read block positions
  unsigned long long* blockPos = new unsigned long long[nrOfCols];
  myfile.read((char*) blockPos, nrOfCols * 8);  // nrOfCols file positions


  // Determine column selection
  int *colIndex;
  int nrOfSelect = LENGTH(columnSelection);

  if (Rf_isNull(columnSelection))
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
    colIndex = new int[nrOfSelect];
    int equal;
    for (int colSel = 0; colSel < nrOfSelect; ++colSel)
    {
      equal = -1;
      const char* str1 = CHAR(STRING_ELT(columnSelection, colSel));

      for (int colNr = 0; colNr < nrOfCols; ++colNr)
      {
        const char* str2 = CHAR(STRING_ELT(colNames, colNr));
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
        myfile.close();
        UNPROTECT(1);
        ::Rf_error("Selected column not found.");
      }

      colIndex[colSel] = equal;
    }
  }


  // Check range of selected rows
  int firstRow = INTEGER(startRow)[0] - 1;
  int nrOfRows = *chunkRows;  // TODO: check for row numbers > INT_MAX !!!

  if (firstRow >= nrOfRows || firstRow < 0)
  {
    delete[] metaDataBlock;
    delete[] blockPos;
    myfile.close();
    UNPROTECT(1);

    if (firstRow < 0)
    {
      ::Rf_error("Parameter fromRow should have a positive value.");
    }

    ::Rf_error("Row selection is out of range.");
  }

  int length = nrOfRows - firstRow;


  // Determine vector length
  if (!Rf_isNull(endRow))
  {
    int lastRow = *INTEGER(endRow);

    if (lastRow <= firstRow)
    {
      delete[] metaDataBlock;
      delete[] blockPos;
      myfile.close();
      UNPROTECT(1);
      ::Rf_error("Parameter 'lastRow' should be equal to or larger than parameter 'fromRow'.");
    }

    length = min(lastRow - firstRow, nrOfRows - firstRow);
  }

  // Vector of selected column names
  SEXP selectedNames;
  PROTECT(selectedNames = Rf_allocVector(STRSXP, nrOfSelect));

  SEXP resTable;
  PROTECT(resTable = Rf_allocVector(VECSXP, nrOfSelect));

  BlockReaderChar* blockReaderStrVec = new BlockReaderChar();

  for (int colSel = 0; colSel < nrOfSelect; ++colSel)
  {
    int colNr = colIndex[colSel];

    if (colNr < 0 || colNr >= nrOfCols)
    {
      delete[] metaDataBlock;
      delete[] blockPos;
      myfile.close();
      UNPROTECT(4);
      ::Rf_error("Column selection is out of range.");
    }

    // Column name
    SEXP selName = STRING_ELT(colNames, colNr);
    SET_STRING_ELT(selectedNames, colSel, selName);

    unsigned long long pos = blockPos[colNr];

    switch (colTypes[colNr])
    {
    // Character vector
      case 6:
        // PROTECT(strVec = Rf_allocVector(STRSXP, length));
        // blockReaderStrVec = new BlockReaderChar();
        fdsReadCharVec_v6(myfile, (IBlockReader*) blockReaderStrVec, pos, firstRow, length, nrOfRows);

        SET_VECTOR_ELT(resTable, colSel, blockReaderStrVec->StrVector());
        UNPROTECT(1);
        break;

      // Integer vector
      case 8:
        SEXP intVec;
        PROTECT(intVec = Rf_allocVector(INTSXP, length));
        fdsReadIntVec_v8(myfile, INTEGER(intVec), pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, intVec);
        UNPROTECT(1);
        break;

      // Real vector
      case 9:
        SEXP realVec;
        PROTECT(realVec = Rf_allocVector(REALSXP, length));
        fdsReadRealVec_v9(myfile, REAL(realVec), pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, realVec);
        UNPROTECT(1);
        break;

      // Logical vector
      case 10:
        SEXP boolVec;
        PROTECT(boolVec = Rf_allocVector(LGLSXP, length));
        fdsReadLogicalVec_v10(myfile, LOGICAL(boolVec), pos, firstRow, length, nrOfRows);
        SET_VECTOR_ELT(resTable, colSel, boolVec);
        UNPROTECT(1);
        break;

      // Factor vector
      case 7:
        SEXP facVec;
        PROTECT(facVec = Rf_allocVector(INTSXP, length));

        fdsReadFactorVec_v7(myfile, (IBlockReader*) blockReaderStrVec, INTEGER(facVec), pos, firstRow, length, nrOfRows);

        Rf_setAttrib(facVec, Rf_mkString("levels"), blockReaderStrVec->StrVector());
        Rf_setAttrib(facVec, Rf_mkString("class"), Rf_mkString("factor"));

        SET_VECTOR_ELT(resTable, colSel, facVec);
        UNPROTECT(2);  // level string was also generated
        break;


      default:
        delete[] metaDataBlock;
        delete[] blockPos;

        myfile.close();
        ::Rf_error("Unknown type found in column.");
    }
  }

  delete blockReaderStrVec;

  myfile.close();

  // Generalize to full atributes
  Rf_setAttrib(resTable, R_NamesSymbol, selectedNames);

  int found = 0;
  for (int i = 0; i < keyLength; ++i)
  {
    for (int colSel = 0; colSel < nrOfSelect; ++colSel)
    {
      if (keyColPos[i] == colIndex[colSel])  // key present in result
      {
        ++found;
        break;
      }
    }
  }

  // Only when keys are present in result set
  if (found > 0)
  {
    SEXP keyNames;
    PROTECT(keyNames = Rf_allocVector(STRSXP, found));
    for (int i = 0; i < found; ++i)
    {
      SET_STRING_ELT(keyNames, i, STRING_ELT(colNames, keyColPos[i]));
    }

    // cleanup
    UNPROTECT(4);
    delete[] metaDataBlock;
    delete[] blockPos;

    return List::create(
      _["keyNames"] = keyNames,
      _["found"] = found,
      _["resTable"] = resTable,
      _["selectedNames"] = selectedNames);
  }

  UNPROTECT(3);
  delete[] metaDataBlock;
  delete[] blockPos;

  // Convert to integer vector
  IntegerVector keyColVec(keyLength + 1);
  for (int col = 0; col != keyLength; ++col)
  {
    keyColVec[col] = keyColPos[col];
  }

  return List::create(
    _["keyNames"] = R_NilValue,
    _["found"] = found,
    _["keyColVec"] = keyColVec,
    _["keyLength"] = keyLength,
    _["nrOfSelect"] = nrOfSelect,
    _["selectedNames"] = selectedNames,
    _["resTable"] = resTable);
}
