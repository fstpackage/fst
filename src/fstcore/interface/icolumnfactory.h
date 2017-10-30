/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  BSD 3-Clause (https://opensource.org/licenses/BSD-3-Clause)

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:

  1. Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.

  2. Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.

  3. Neither the name of the copyright holder nor the names of its
     contributors may be used to endorse or promote products derived from
     this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
  PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

  You can contact the author at :
  - fstlib source repository : https://github.com/fstPackage/fstlib
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

