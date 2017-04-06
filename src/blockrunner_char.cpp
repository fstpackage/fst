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



#include "iblockrunner.h"
#include "blockrunner_char.h"
#include "fstdefines.h"

#include <Rcpp.h>


using namespace std;
using namespace Rcpp;


BlockRunner::BlockRunner(SEXP &strVec, unsigned int* strSizes, unsigned int* naInts, char* stackBuf, unsigned int stackBufSize)
{
  this->strVec = &strVec;
  this->naInts = naInts;
  this->strSizes = strSizes;
  this->buf = stackBuf;
  this->stackBufSize = stackBufSize;

  heapBufSize = BASIC_HEAP_SIZE;
  heapBuf = new char[BASIC_HEAP_SIZE];
}


BlockRunner::~BlockRunner()
{
  delete[] heapBuf;
}


void BlockRunner::RetrieveBlock(unsigned int startCount, unsigned int endCount)
{
  // Determine string lengths
  // unsigned int startCount = block * BLOCKSIZE_CHAR;
  unsigned int nrOfElements = endCount - startCount;  // the string at position endCount is not included
  unsigned int nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag

  unsigned int totSize = 0;
  unsigned int hasNA = 0;
  int sizeCount = -1;

  memset(naInts, 0, nrOfNAInts * 4);  // clear NA bit metadata block (neccessary?)

  for (unsigned int count = startCount; count != endCount; ++count)
  {
    SEXP strElem = STRING_ELT(*strVec, count);

    if (strElem == NA_STRING)  // set NA bit
    {
      ++hasNA;
      unsigned int intPos = (count - startCount) / 32;
      unsigned int bitPos = (count - startCount) % 32;
      naInts[intPos] |= 1 << bitPos;
    }

    totSize += LENGTH(strElem);
    strSizes[++sizeCount] = totSize;
  }

  if (hasNA != 0)  // NA's present in vector
  {
    // set last bit
    int bitPos = nrOfElements % 32;
    naInts[nrOfNAInts - 1] |= 1 << bitPos;
  }


  // Write string data
  unsigned int pos = 0;
  unsigned int lastPos = 0;
  sizeCount = -1;

  activeBuf = buf;

  if (totSize > stackBufSize)  // don't use cache memory
  {
    if (totSize > heapBufSize)
    {
      delete[] heapBuf;
      heapBufSize = totSize * 1.1;
      heapBuf = new char[heapBufSize];
    }

    activeBuf = heapBuf;
  }

  for (unsigned int count = startCount; count < endCount; ++count)
  {
    const char* str = CHAR(STRING_ELT(*strVec, count));
    pos = strSizes[++sizeCount];
    strncpy(activeBuf + lastPos, str, pos - lastPos);
    lastPos = pos;
  }

  bufSize = totSize;
}
