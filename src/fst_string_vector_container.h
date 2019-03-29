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

#ifndef STRING_VECTOR_CONTAINER_H
#define STRING_VECTOR_CONTAINER_H


#include <stdexcept>
#include <memory>

#include <Rcpp.h>

#include <interface/fstdefines.h>


// Class is meant for smaller character vectors that need protection from garbage collection

class StringVectorContainer : public IStringColumn
{
  SEXP container;  // make sure the contained object is PROTECTED
  std::unique_ptr<BlockReaderChar> str_vector_p;

public:
  StringVectorContainer(SEXP container_list)
  {
    container = container_list;
  }

  ~StringVectorContainer(){}

  void AllocateVec(unsigned long long vecLength)
  {
    str_vector_p = std::unique_ptr<BlockReaderChar>(new BlockReaderChar());  // unprotected SEXP
    BlockReaderChar* block_reader = str_vector_p.get();

    // allocate
    block_reader->AllocateVec(vecLength);

    // make sure vector is protected
    SEXP str_vec = block_reader->StrVector();
    SET_VECTOR_ELT(container, 0, str_vec);
  }

  StringEncoding GetEncoding()
  {
    return str_vector_p.get()->GetEncoding();
  }

  void SetEncoding(StringEncoding fst_string_encoding)
  {
    str_vector_p.get()->SetEncoding(fst_string_encoding);
  }

  void BufferToVec(unsigned long long nrOfElements, unsigned long long startElem, unsigned long long endElem,
    unsigned long long vecOffset, unsigned int* sizeMeta, char* buf)
  {
    str_vector_p.get()->BufferToVec(nrOfElements, startElem, endElem, vecOffset, sizeMeta, buf);
  }

  const char* GetElement(unsigned long long elementNr)
  {
    return str_vector_p.get()->GetElement(elementNr);
  }

  SEXP StrVector()
  {
    return VECTOR_ELT(container, 0);
  }
};


#endif  // STRING_VECTOR_CONTAINER_H
