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


#include <memory>

#include <Rcpp.h>

#include <interface/istringwriter.h>
#include <interface/ifstcolumn.h>
#include <interface/fstdefines.h>

#include <fst_blockrunner_char.h>


using namespace std;
using namespace Rcpp;


BlockWriterChar::BlockWriterChar(SEXP &strVec, uint64_t vecLength, unsigned int stackBufSize, int uniformEncoding)
{
  this->strVec = &strVec;
  this->naInts = naInts_buf;
  this->strSizes = strSizes_buf;
  this->stackBufSize = stackBufSize;
  this->vecLength = vecLength;
  this->uniformEncoding = uniformEncoding;

  heapBufSize = BASIC_HEAP_SIZE;

  heapBuf = std::unique_ptr<char[]>(new char[BASIC_HEAP_SIZE]);
}


BlockWriterChar::~BlockWriterChar()
{
}


void BlockWriterChar::SetBuffersFromVec(uint64_t startCount, uint64_t endCount)
{
  // Determine string lengths
  // unsigned int startCount = block * BLOCKSIZE_CHAR;
  uint64_t nrOfElements = endCount - startCount;  // the string at position endCount is not included
  uint64_t nrOfNAInts = 1 + nrOfElements / 32;  // add 1 bit for NA present flag

  uint64_t totSize = 0;
  unsigned int hasNA = 0;
  long long sizeCount = -1;

  memset(naInts, 0, nrOfNAInts * 4);  // clear NA bit metadata block (neccessary?)

  for (uint64_t count = startCount; count != endCount; ++count)
  {
    SEXP strElem = STRING_ELT(*strVec, count);

    if (strElem == NA_STRING)  // set NA bit
    {
      ++hasNA;
      uint64_t intPos = (count - startCount) / 32;
      uint64_t bitPos = (count - startCount) % 32;
      naInts[intPos] |= 1 << bitPos;
    }

    totSize += LENGTH(strElem);
    strSizes[++sizeCount] = totSize;
  }

  if (hasNA != 0)  // NA's present in vector
  {
    // set last bit
    uint64_t bitPos = nrOfElements % 32;
    naInts[nrOfNAInts - 1] |= 1 << bitPos;
  }


  // Write string data
  uint64_t pos;
  uint64_t lastPos = 0;
  sizeCount = -1;

  activeBuf = buf;

  // This needs better memory management, for example a memory pool
  if (totSize > stackBufSize)  // don't use cache memory
  {
    if (totSize > heapBufSize)
    {
      heapBufSize = totSize * 1.1;
      heapBuf = std::unique_ptr<char[]>(new char[heapBufSize]);
    }

    activeBuf = heapBuf.get();
  }

  for (uint64_t count = startCount; count < endCount; ++count)
  {
    const char* str = CHAR(STRING_ELT(*strVec, count));
    pos = strSizes[++sizeCount];
    strncpy(activeBuf + lastPos, str, pos - lastPos);
    lastPos = pos;
  }

  bufSize = totSize;
}


void BlockReaderChar::AllocateVec(uint64_t vecLength)
{
  this->strVec = Rf_allocVector(STRSXP, vecLength);
  // isProtected = true;
}

void BlockReaderChar::BufferToVec(uint64_t nrOfElements, uint64_t startElem,
  uint64_t endElem, uint64_t vecOffset, unsigned int* sizeMeta, char* buf)
{
  uint64_t nrOfNAInts = 1 + nrOfElements / 32;  // last bit is NA flag
  unsigned int* bitsNA = &sizeMeta[nrOfElements];
  uint64_t pos = 0;

  if (startElem != 0)
  {
    pos = sizeMeta[startElem - 1];  // offset previous element
  }

  // Test NA flag TODO: set as first flag and remove parameter nrOfNAInts
  unsigned int flagNA = bitsNA[nrOfNAInts - 1] & (1 << (nrOfElements % 32));
  if (flagNA == 0)  // no NA's in vector
  {
    for (uint64_t blockElem = startElem; blockElem <= endElem; ++blockElem)
    {
      uint64_t newPos = sizeMeta[blockElem];
      SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
      pos = newPos;  // update to new string offset
    }

    return;
  }

  // We process the datablock in cycles of 32 strings. This minimizes the impact of NA testing for vectors with a small number of NA's

  uint64_t startCycle = startElem / 32;
  uint64_t endCycle = endElem / 32;
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

      uint64_t newPos = sizeMeta[blockElem];
      SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
      pos = newPos;  // update to new string offset
    }

    return;
  }

  // Get possibly partial first cycle

  uint64_t firstCylceEnd = startCycle * 32 + 31;
  for (uint64_t blockElem = startElem; blockElem <= firstCylceEnd; ++blockElem)
  {
    unsigned int bitMask = 1 << (blockElem % 32);

    if ((cycleNAs & bitMask) != 0)  // set string to NA
    {
      SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, NA_STRING);
      pos = sizeMeta[blockElem];  // update to new string offset
      continue;
    }

    // Get string from data stream

    uint64_t newPos = sizeMeta[blockElem];
    SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
    SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
    pos = newPos;  // update to new string offset
  }

  // Get all but last cycle with fast NA test

  for (uint64_t cycle = startCycle + 1; cycle != endCycle; ++cycle)
  {
    unsigned int cycleNAs = bitsNA[cycle];
    uint64_t middleCycleEnd = cycle * 32 + 32;

    if (cycleNAs == 0)  // no NA's
    {
      for (uint64_t blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
      {
        uint64_t newPos = sizeMeta[blockElem];
        SEXP curStr = Rf_mkCharLenCE(buf + pos, newPos - pos, this->strEncoding);
        SET_STRING_ELT(strVec, vecOffset + blockElem - startElem, curStr);
        pos = newPos;  // update to new string offset
      }
      continue;
    }

    // Cycle contains one or more NA's

    for (uint64_t blockElem = cycle * 32; blockElem != middleCycleEnd; ++blockElem)
    {
      unsigned int bitMask = 1 << (blockElem % 32);
      uint64_t newPos = sizeMeta[blockElem];

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
  for (uint64_t blockElem = endCycle * 32; blockElem != endElem; ++blockElem)
  {
    unsigned int bitMask = 1 << (blockElem % 32);
    uint64_t newPos = sizeMeta[blockElem];

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

