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


#ifndef IFST_COLUMN_FACTORY_H
#define IFST_COLUMN_FACTORY_H


#include "ifstcolumn.h"

class IColumnFactory
{
public:
  virtual ~IColumnFactory() {};
  virtual IFactorColumn*  CreateFactorColumn(int nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual ILogicalColumn* CreateLogicalColumn(int nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual IDoubleColumn* CreateDoubleColumn(int nrOfRows, FstColumnAttribute columnAttribute, short int scale) = 0;
  virtual IIntegerColumn* CreateIntegerColumn(int nrOfRows, FstColumnAttribute columnAttribute, short int scale) = 0;
  virtual IByteColumn* CreateByteColumn(int nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual IInt64Column* CreateInt64Column(int nrOfRows, FstColumnAttribute columnAttribute, short int scale) = 0;
  virtual IStringColumn* CreateStringColumn(int nrOfRows, FstColumnAttribute columnAttribute) = 0;
  virtual IStringArray* CreateStringArray() = 0;
};

#endif // IFST_COLUMN_FACTORY_H

