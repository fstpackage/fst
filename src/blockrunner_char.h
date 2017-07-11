/*
  fst - An R-package for ultra fast storage and retrieval of datasets.
  Header File
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

#ifndef BLOCKRUNNER_CHAR_H
#define BLOCKRUNNER_CHAR_H


#include <Rcpp.h>

#include <interface/fstdefines.h>


class BlockWriterChar : public IStringWriter
{
  SEXP* strVec;
  unsigned int stackBufSize;
  unsigned int heapBufSize;
  // char* buf;
  char *heapBuf;

  // Buffers for blockRunner (make these local !!!!)
  unsigned int naInts_buf[1 + BLOCKSIZE_CHAR / 32];  // we have 32 NA bits per integer
  unsigned int strSizes_buf[BLOCKSIZE_CHAR];  // we have 32 NA bits per integer
  char buf[MAX_CHAR_STACK_SIZE];

  public:
    BlockWriterChar(SEXP &strVec, unsigned int vecLength, unsigned int stackBufSize);

    ~BlockWriterChar();

    StringEncoding Encoding()
    {
      // Get first non-NA element and set encoding accordingly
      // (expensive for vectors with lots of NA's at the start, solve!)

      cetype_t encoding = cetype_t::CE_NATIVE;  // default
      for (unsigned int pos = 0; pos < this->vecLength; pos++)
      {
        SEXP strElem = STRING_ELT(*strVec, pos);
        if (strElem != NA_STRING)  // set NA bit
        {
          encoding = Rf_getCharCE(strElem);
        }
      }

      switch (encoding)
      {
        case cetype_t::CE_LATIN1:
          return StringEncoding::LATIN1;

        case cetype_t::CE_UTF8:
          return StringEncoding::UTF8;

        default:
          return StringEncoding::NATIVE;
      }
    }

    void SetBuffersFromVec(unsigned int startCount, unsigned int endCount);
};


class BlockReaderChar : public IStringColumn
{
  SEXP strVec;
  bool isProtected;
  cetype_t strEncoding;

public:
  BlockReaderChar() { isProtected = true; }

  ~BlockReaderChar(){ if (isProtected) UNPROTECT(1); }

  void AllocateVec(unsigned int vecLength);

  void SetEncoding(StringEncoding stringEncoding)
  {
    switch (stringEncoding)
    {
      case StringEncoding::LATIN1:
      {
        strEncoding = cetype_t::CE_LATIN1;
        break;
      }

      case StringEncoding::UTF8:
      {
        strEncoding = cetype_t::CE_UTF8;
        break;
      }

      default:  // native or unknown encoding
        strEncoding = cetype_t::CE_NATIVE;
        break;
    }
  }

  void BufferToVec(unsigned int nrOfElements, unsigned int startElem, unsigned int endElem,
    unsigned int vecOffset, unsigned int* sizeMeta, char* buf);

  const char* GetElement(int elementNr)
  {
    return CHAR(STRING_ELT(strVec, elementNr));
  }

  SEXP StrVector() { return strVec; }
};



#endif  // BLOCKRUNNER_CHAR_H
