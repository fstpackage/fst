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


#ifndef FST_STORE_H
#define FST_STORE_H


#include <vector>

#include <interface/icolumnfactory.h>
#include <interface/ifsttable.h>


class FstStore
{
  unsigned int tableClassType;
  int nrOfColsFirstChunk;

  std::string fstFile;

  char* metaDataBlock;

  public:
    unsigned long long* p_nrOfRows;
    int* keyColPos;
    unsigned short int* colTypes;
    unsigned int version;
    int nrOfCols, keyLength;
    IStringColumn* blockReader;

    FstStore(std::string fstFile);

    ~FstStore() { delete[] metaDataBlock; delete blockReader; }

	/**
     * \brief Stream a data table
     * \param fstTable Table to stream, implementation of IFstTable interface
     * \param compress Compression factor with a value 0-100
     */
    void fstWrite(IFstTable &fstTable, int compress) const;

    int fstMeta(IColumnFactory* columnFactory);

    int fstRead(IFstTable &tableReader, IStringArray* columnSelection, int startRow, int endRow, IColumnFactory* columnFactory, std::vector<int> &keyIndex, IStringArray* selectedCols);
};


#endif  // FST_STORE_H
