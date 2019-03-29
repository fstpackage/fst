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


#ifndef TYPE_FACTORY_H
#define TYPE_FACTORY_H


#include <interface/itypefactory.h>


class BlobContainer : public IBlobContainer
{
  SEXP rawVec;

public:
  BlobContainer(unsigned long long size)
  {
    // code was carefully examined to assert that no PROTECT is needed here
    rawVec = Rf_allocVector(RAWSXP, size);
  }

  ~BlobContainer()
  {
  }

  unsigned char* Data()
  {
    return RAW(rawVec);
  }

  SEXP RVector()
  {
    return rawVec;
  }

  unsigned long long Size()
  {
    return Rf_xlength(rawVec);
  }
};


class TypeFactory : public ITypeFactory
{
public:
  ~TypeFactory() { }

  /**
   * \brief Create a BlobContainer type of size indicated.
   * \param size Size of BlobContainer to create.
   * \return Pointer to the generated BlobContainer object;
   */
  IBlobContainer* CreateBlobContainer(unsigned long long size)
  {
    return new BlobContainer(size);
  }
};


#endif  // TYPE_FACTORY_H
