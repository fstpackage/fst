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


#include <interface/istringwriter.h>
#include <interface/ifstcolumn.h>
#include <interface/fstdefines.h>

#include <fst_blockrunner_char.h>

#include <Rcpp.h>


using namespace std;
using namespace Rcpp;


BlockWriterChar::BlockWriterChar(SEXP &strVec, unsigned long long vecLength, unsigned int stackBufSize, int uniformEncoding)
{
  this->strVec = &strVec;
  this->naInts = naInts_buf;
  this->strSizes = strSizes_buf;
  this->stackBufSize = stackBufSize;
  this->vecLength = vecLength;
  this->uniformEncoding = uniformEncoding;

  heapBufSize = BASIC_HEAP_SIZE;
  heapBuf = new char[BASIC_HEAP_SIZE];
}


BlockWriterChar::~BlockWriterChar()
{
  delete[] heapBuf;
}


void BlockWriterChar::SetBuffersFromVec(unsigned long long startCount, unsigned long long endCount)
{
  // Determine string lengths
  // unsigned int startCount = block * BLOCKSIZE_CHAR;
  unsigned long long nrOfElements = endCount - startCount;  // the string at position endCount is not included
  unsigned long long nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag

  unsigned long long totSize = 0;
  unsigned int hasNA = 0;
  long long sizeCount = -1;

  memset(naInts, 0, nrOfNAInts * 4);  // clear NA bit metadata block (neccessary?)

  for (unsigned long long count = startCount; count != endCount; ++count)
  {
    SEXP strElem = STRING_ELT(*strVec, count);

    if (strElem == NA_STRING)  // set NA bit
    {
      ++hasNA;
      unsigned long long intPos = (count - startCount) / 32;
      unsigned long long bitPos = (count - startCount) % 32;
      naInts[intPos] |= 1 << bitPos;
    }

    totSize += LENGTH(strElem);
    strSizes[++sizeCount] = totSize;
  }

  if (hasNA != 0)  // NA's present in vector
  {
    // set last bit
    unsigned long long bitPos = nrOfElements % 32;
    naInts[nrOfNAInts - 1] |= 1 << bitPos;
  }


  // Write string data
  unsigned long long pos;
  unsigned long long lastPos = 0;
  sizeCount = -1;

  activeBuf = buf;

  // This needs better memory management, for example a memory pool
  if (totSize > stackBufSize)  // don't use cache memory
  {
    if (totSize > heapBufSize)
    {
      delete[] heapBuf;
      heapBuf = nullptr;
      heapBufSize = totSize * 1.1;
      heapBuf = new char[heapBufSize];
    }

    activeBuf = heapBuf;
  }

  for (unsigned long long count = startCount; count < endCount; ++count)
  {
    const char* str = CHAR(STRING_ELT(*strVec, count));
    pos = strSizes[++sizeCount];
    strncpy(activeBuf + lastPos, str, pos - lastPos);
    lastPos = pos;
  }

  bufSize = totSize;
}


void BlockReaderChar::AllocateVec(unsigned long long vecLength)
{
  PROTECT(this->strVec = Rf_allocVector(STRSXP, vecLength));
  isProtected = true;
}

void BlockReaderChar::BufferToVec(unsigned long long nrOfElements, unsigned long long startElem,
  unsigned long long endElem, unsigned long long vecOffset, unsigned int* sizeMeta, char* buf)
{
  unsigned long long nrOfNAInts = 1 + nrOfElements / 32;  // last bit is NA flag
  unsigned int* bitsNA = &sizeMeta[nrOfElements];
  unsigned long long pos = 0;

  if (startElem != 0)
  {
    pos = sizeMeta[startElem - 1];  // offset previous element
  }

  // Test NA flag TODO: set as first flag and remove parameter nrOfNAInts
  unsigned int flagNA = bitsNA[nrOfNAInts - 1] & (1 << (nrOfElements % 32));
  if (flagNA == 0)  // no NA's in vector
  {
    for (unsigned long long blockElem = startElem; blockElem <= endElem; ++blockElem)
    {
      unsigned long long newPos = sizeMeta[blockElem];
      SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
      pos = newPos;  // update to new string offset
    }

    return;
  }

  // We process the datablock in cycles of 32 strings. This minimizes the impact of NA testing for vectors with a small number of NA's

  unsigned long long startCycle = startElem / 32;
  unsigned long long endCycle = endElem / 32;
  unsigned int cycleNAs = bitsNA[startCycle];

  // A single 32 string cycle

  if (startCycle == endCycle)
  {
    for (unsigned int blockElem = startElem; blockElem <= endElem; ++blockElem)
    {
      unsigned int bitMask = 1 << (blockElem % 32);

      if ((cycleNAs & bitMask) != 0)  // set string to NA
      {
        SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, NA_STRING);
        pos = sizeMeta[blockElem];  // update to new string offset
        continue;
      }

      // Get string from data stream

      unsigned long long newPos = sizeMeta[blockElem];
      SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
      pos = newPos;  // update to new string offset
    }

    return;
  }

  // Get possibly partial first cycle

  unsigned long long firstCylceEnd = startCycle * 32 + 31;
  for (unsigned long long blockElem = startElem; blockElem <= firstCylceEnd; ++blockElem)
  {
    unsigned int bitMask = 1 << (blockElem % 32);

    if ((cycleNAs & bitMask) != 0)  // set string to NA
    {
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, NA_STRING);
      pos = sizeMeta[blockElem];  // update to new string offset
      continue;
    }

    // Get string from data stream

    unsigned long long newPos = sizeMeta[blockElem];
    SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
    SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
    pos = newPos;  // update to new string offset
  }

  // Get all but last cycle with fast NA test

  for (unsigned long long cycle = startCycle + 1; cycle != endCycle; ++cycle)
  {
    unsigned int cycleNAs = bitsNA[cycle];
    unsigned long long middleCycleEnd = cycle * 32 + 32;

    if (cycleNAs == 0)  // no NA's
    {
      for (unsigned long long blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
      {
        unsigned long long newPos = sizeMeta[blockElem];
        SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
        SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
        pos = newPos;  // update to new string offset
      }
      continue;
    }

    // Cycle contains one or more NA's

    for (unsigned long long blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
    {
      unsigned int bitMask = 1 << (blockElem % 32);
      unsigned long long newPos = sizeMeta[blockElem];

      if ((cycleNAs & bitMask) != 0)  // set string to NA
      {
        SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, NA_STRING);
        pos = newPos;  // update to new string offset
        continue;
      }

      // Get string from data stream

      SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
      pos = newPos;  // update to new string offset
    }
  }

  // Last cycle

  cycleNAs = bitsNA[endCycle];

  ++endElem;
  for (unsigned long long blockElem = endCycle * 32; blockElem != endElem; ++blockElem)
  {
    unsigned int bitMask = 1 << (blockElem % 32);
    unsigned long long newPos = sizeMeta[blockElem];

    if ((cycleNAs & bitMask) != 0)  // set string to NA
    {
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, NA_STRING);
      pos = newPos;  // update to new string offset
      continue;
    }

    // Get string from data stream

    SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
    SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
    pos = newPos;  // update to new string offset
  }
}

