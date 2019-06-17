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


#ifndef COLUMN_FACTORY_H
#define COLUMN_FACTORY_H

#include <iostream>
#include <vector>

#include <Rcpp.h>

#include <interface/icolumnfactory.h>
#include <interface/ifstcolumn.h>
#include <interface/ifsttable.h>

#include <fst_column.h>
#include <fst_blockrunner_char.h>


class ColumnFactory : public IColumnFactory
{
public:
  ~ColumnFactory() {};

  IFactorColumn* CreateFactorColumn(uint64_t nrOfRows, uint64_t nrOfLevels, FstColumnAttribute columnAttribute)
  {
    return new FactorColumn(nrOfRows, nrOfLevels, columnAttribute);
  }

  ILogicalColumn* CreateLogicalColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute)
  {
    return new LogicalColumn(nrOfRows);
  }

  IDoubleColumn* CreateDoubleColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    return new DoubleColumn(nrOfRows, columnAttribute, scale);
  }

  IIntegerColumn* CreateIntegerColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    return new IntegerColumn(nrOfRows, columnAttribute, scale);
  }

  IByteColumn* CreateByteColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute)
  {
    return new ByteColumn(nrOfRows);
  }

  IInt64Column* CreateInt64Column(uint64_t nrOfRows, FstColumnAttribute columnAttribute, short int scale)
  {
    return new Int64Column(nrOfRows, columnAttribute, scale);
  }

  IStringColumn* CreateStringColumn(uint64_t nrOfRows, FstColumnAttribute columnAttribute)
  {
    return new BlockReaderChar();
  }

  IStringArray* CreateStringArray()
  {
    return new StringArray();
  }
};


#endif  // COLUMN_FACTORY_H

