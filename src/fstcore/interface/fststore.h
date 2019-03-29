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


#ifndef FST_STORE_H
#define FST_STORE_H


#include <vector>
#include <memory>

#include <interface/icolumnfactory.h>
#include <interface/ifsttable.h>


class FstStore
{
  std::string fstFile;
  std::unique_ptr<char[]> metaDataBlockP;

  public:
    unsigned long long* p_nrOfRows;
    int* keyColPos;

    char* metaDataBlock;

  	// column info
    unsigned short int* colTypes;
    unsigned short int* colBaseTypes;
    unsigned short int* colAttributeTypes;
    unsigned short int* colScales;

    unsigned int tableVersionMax;
    int nrOfCols, keyLength;

    FstStore(std::string fstFile);

    ~FstStore() { }

	/**
     * \brief Stream a data table
     * \param fstTable Table to stream, implementation of IFstTable interface
     * \param compress Compression factor with a value 0-100
     */
    void fstWrite(IFstTable &fstTable, int compress) const;

    void fstMeta(IColumnFactory* columnFactory, IStringColumn* col_names);

    void fstRead(IFstTable &tableReader, IStringArray* columnSelection, long long startRow, long long endRow,
      IColumnFactory* columnFactory, std::vector<int> &keyIndex, IStringArray* selectedCols, IStringColumn* col_names);
};


#endif  // FST_STORE_H
