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


class BlockWriterChar : public IBlockWriter
{
  SEXP* strVec;
  unsigned int stackBufSize;
  unsigned int heapBufSize;
  char* buf;
  char *heapBuf;

  public:
    BlockWriterChar(SEXP &strVec, unsigned int* strSizes, unsigned int* naInts, char* stackBuf, unsigned int stackBufSize);
    ~BlockWriterChar();

    void SetBuffersFromVec(unsigned int startCount, unsigned int endCount);
};


class BlockReaderChar : public IBlockReader
{
  SEXP strVec;

public:
  ~BlockReaderChar();

  void AllocateVec(unsigned int vecLength);

  void BufferToVec(unsigned int nrOfElements, unsigned int startElem, unsigned int endElem,
    unsigned int vecOffset, unsigned int* sizeMeta, char* buf);

  SEXP StrVector() { return strVec; }
};



#endif  // BLOCKRUNNER_CHAR_H
