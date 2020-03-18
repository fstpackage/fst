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
  SEXP raw_vec;

public:
  BlobContainer(unsigned long long size, SEXP r_container)
  {
    // PROTECT raw vector by containing it in a list object
    raw_vec = Rf_allocVector(RAWSXP, size);
    SET_VECTOR_ELT(r_container, 0, raw_vec);
  }

  ~BlobContainer()
  {
  }

  unsigned char* Data()
  {
    return RAW(raw_vec);
  }

  SEXP RVector()
  {
    return raw_vec;
  }

  unsigned long long Size()
  {
    return Rf_xlength(raw_vec);
  }
};


class TypeFactory : public ITypeFactory
{
  SEXP r_container;
  bool has_blob = false;

public:

  TypeFactory(SEXP r_container)
  {
    this->r_container = r_container;
  }

  ~TypeFactory() { }

  /**
   * \brief Create a BlobContainer type of size indicated.
   * \param size Size of BlobContainer to create.
   * \return Pointer to the generated BlobContainer object;
   */
  IBlobContainer* CreateBlobContainer(unsigned long long size)
  {
    if (this->has_blob) {
      Rf_error("Blob container can be used only once");
    }

    return new BlobContainer(size, r_container);
  }
};


#endif  // TYPE_FACTORY_H
