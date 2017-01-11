/*
  fst - An R-package for ultra fast storage and retrieval of datasets.
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

#include <Rcpp.h>
#include <R.h>
#include <Rinternals.h>

using namespace Rcpp;
using namespace std;

// [[Rcpp::export]]
int SType(SEXP value)
{
  return TYPEOF(value);
}

// [[Rcpp::export]]
int compChar(SEXP str1, SEXP str2)
{
  const char* strA = CHAR(STRING_ELT(str1, 0));
  const char* strB = CHAR(STRING_ELT(str2, 0));

  return strcmp(strA, strB);
}


// When calling, use 'lower' 1 smaller than the first element that you want to check. That way, when the result == lower - 1,
// you know that all elements are larger than the key.
int lastIntEqualLower(int* intVec, int key, int lower, int upper)
{
  if ((upper - lower) < 2)
  {
    if (intVec[upper] <= key) return upper;
    return lower;
  }

  int pos = (upper + lower) / 2;  // select new element

  if (intVec[pos] > key)
  {
    return lastIntEqualLower(intVec, key, lower, pos - 1);
  }

  return lastIntEqualLower(intVec, key, pos, upper);
}


// When calling, use 'upper' 1 larger than the last element you want to check. That way, when result == upper,
// you know that all elements are smaller than the key.
int firstIntEqualHigher(int* intVec, int key, int lower, int upper)
{
  if ((upper - lower) < 2)
  {
    if (intVec[lower] >= key) return lower;
    return upper;
  }

  int pos = (upper + lower) /2;  // select new element

  if (intVec[pos] < key)
  {
    return firstIntEqualHigher(intVec, key, pos + 1, upper);
  }

  return firstIntEqualHigher(intVec, key, lower, pos);
}


int lastDoubleEqualLower(double* doubleVec, double key, int lower, int upper)
{
  if ((upper - lower) < 2)
  {
    if (doubleVec[upper] <= key) return upper;
    return lower;
  }

  int pos = (upper + lower) / 2;  // select new element

  if (doubleVec[pos] > key)
  {
    return lastDoubleEqualLower(doubleVec, key, lower, pos - 1);
  }

  return lastDoubleEqualLower(doubleVec, key, pos, upper);
}


int firstDoubleEqualHigher(double* doubleVec, double key, int lower, int upper)
{
  if ((upper - lower) < 2)
  {
    if (doubleVec[lower] >= key) return lower;
    return upper;
  }

  int pos = (upper + lower) / 2;  // select new element

  if (doubleVec[pos] < key)
  {
    return firstDoubleEqualHigher(doubleVec, key, pos + 1, upper);
  }

  return firstDoubleEqualHigher(doubleVec, key, lower, pos);
}


int lastCharEqualLower(SEXP charVec, const char* key, int lower, int upper)
{
  if ((upper - lower) < 2)
  {
    const char* str = CHAR(STRING_ELT(charVec, upper));
    if(strcmp(str, key) <= 0) return upper;
    return lower;
  }

  int pos = (upper + lower) / 2;  // select new element

  const char* str = CHAR(STRING_ELT(charVec, pos));
  if(strcmp(str, key) > 0)
  {
    return lastCharEqualLower(charVec, key, lower, pos - 1);
  }

  return lastCharEqualLower(charVec, key, pos, upper);
}


int firstCharEqualHigher(SEXP charVec, const char* key, int lower, int upper)
{
  if ((upper - lower) < 2)
  {
    const char* str = CHAR(STRING_ELT(charVec, lower));
    if(strcmp(str, key) >= 0) return lower;
    return upper;
  }

  int pos = (upper + lower) / 2;  // select new element

  const char* str = CHAR(STRING_ELT(charVec, pos));
  if(strcmp(str, key) < 0)
  {
    return firstCharEqualHigher(charVec, key, pos + 1, upper);
  }

  return firstCharEqualHigher(charVec, key, lower, pos);
}


// Check for data.table
// [[Rcpp::export]]
bool IsSortedTable(SEXP table, SEXP key)
{
  if (TYPEOF(table) != VECSXP) return false;

  SEXP tableClass = Rf_getAttrib(table, R_ClassSymbol);

  if (Rf_isNull(tableClass)) return false;

  // correct class
  if (strcmp(CHAR(STRING_ELT(tableClass, 0)), "data.table") != 0) return false;

  SEXP str;
  str = Rf_allocVector(STRSXP, 1);
  SET_STRING_ELT(str, 0, Rf_mkChar("sorted"));
  SEXP sorted = Rf_getAttrib(table, str);

  if (Rf_isNull(sorted)) return false;

  return true;
}



// [[Rcpp::export]]
SEXP FirstIntEqualHigher(SEXP intVec, SEXP intKey, SEXP lower, SEXP upper)
{
  int res = firstIntEqualHigher(INTEGER(intVec), *INTEGER(intKey), *INTEGER(lower), *INTEGER(upper));

  SEXP intRes;
  PROTECT(intRes = Rf_allocVector(INTSXP, 1));
  *INTEGER(intRes) = res;

  UNPROTECT(1);

  return intRes;
}


// [[Rcpp::export]]
SEXP LastIntEqualLower(SEXP intVec, SEXP intKey, SEXP lower, SEXP upper)
{
  int res = lastIntEqualLower(INTEGER(intVec), *INTEGER(intKey), *INTEGER(lower), *INTEGER(upper));

  SEXP intRes;
  PROTECT(intRes = Rf_allocVector(INTSXP, 1));
  *INTEGER(intRes) = res;

  UNPROTECT(1);

  return intRes;
}



// Search for the first row that has a key equal or larger than the specified key in the range [lower, upper].
// [[Rcpp::export]]
SEXP LowerBoundIndex(SEXP table, SEXP key, SEXP lower, SEXP upper)
{
  // Find the column numbers of the selected columns
  SEXP keyNames = Rf_getAttrib(key, R_NamesSymbol);
  SEXP colNames = Rf_getAttrib(table, R_NamesSymbol);

  int nrOfSelect = LENGTH(keyNames);
  int nrOfCols = LENGTH(colNames);

  int *colIndex = new int[nrOfSelect];

  for (int colSel = 0; colSel < nrOfSelect; ++colSel)
  {
    int equal = -1;
    const char* str1 = CHAR(STRING_ELT(keyNames, colSel));

    for (int colNr = 0; colNr < nrOfCols; ++colNr)
    {
      const char* str2 = CHAR(STRING_ELT(colNames, colNr));
      if (strcmp(str1, str2) == 0)
      {
        equal = colNr;
        break;
      }
    }

    if (equal == -1)
    {
      delete[] colIndex;
      ::Rf_error("Selected key not found.");
    }

    colIndex[colSel] = equal;
  }

  int lowerIndex = (*INTEGER(lower)) - 1;
  int upperIndex = *INTEGER(upper);

  // Binary search on all selected columns
  for (int colSel = 0; colSel < nrOfSelect; ++colSel)
  {
    SEXP colVec = VECTOR_ELT(table, colIndex[colSel]);
    SEXP colKey = VECTOR_ELT(key, colSel);

    bool boundEqual = true;

    switch (TYPEOF(colVec))
    {
      case INTSXP:
      {
        if( Rf_isFactor(colVec)) ::Rf_error("Factors not (yet) allowed.");

        int colKeyInt = *INTEGER(colKey);
        int* intVec = INTEGER(colVec);
        lowerIndex = firstIntEqualHigher(intVec, colKeyInt, lowerIndex, upperIndex);

        if (intVec[lowerIndex] != colKeyInt) boundEqual = false;
        else upperIndex = lastIntEqualLower(intVec, colKeyInt, lowerIndex, upperIndex);

        break;
      }

      case LGLSXP:
      {
        int colKeyInt = *LOGICAL(colKey);
        int* intVec = LOGICAL(colVec);
        lowerIndex = firstIntEqualHigher(intVec, colKeyInt, lowerIndex, upperIndex);

        if (intVec[lowerIndex] != colKeyInt) boundEqual = false;
        else upperIndex = lastIntEqualLower(intVec, colKeyInt, lowerIndex, upperIndex);

        break;
      }

      case REALSXP:
      {
        double colKeyDouble = *REAL(colKey);
        double* doubleVec = REAL(colVec);
        lowerIndex = firstDoubleEqualHigher(doubleVec, colKeyDouble, lowerIndex, upperIndex);

        if (doubleVec[lowerIndex] != colKeyDouble) boundEqual = false;
        else upperIndex = lastDoubleEqualLower(doubleVec, colKeyDouble, lowerIndex, upperIndex);

        break;
      }

      case STRSXP:
      {
        const char* colKeyChar = CHAR(STRING_ELT(colKey, 0));

        lowerIndex = firstCharEqualHigher(colVec, colKeyChar, lowerIndex, upperIndex);

        const char* str = CHAR(STRING_ELT(colVec, lowerIndex));
        if (strcmp(str, colKeyChar) == 0) boundEqual = false;
        else upperIndex = lastCharEqualLower(colVec, colKeyChar, lowerIndex, upperIndex);

        break;
      }

      default:
      {
        delete[] colIndex;
        ::Rf_error("Unknown type found in column.");
      }
    }

    // lowers.push_back(lowerIndex);
    // uppers.push_back(upperIndex);

    if (!boundEqual) break;
  }

  SEXP res;
  res = Rf_allocVector(INTSXP, 1);
  *INTEGER(res) = lowerIndex;

  delete[] colIndex;
  
  return List::create(
    _["keyNames"] = keyNames,
    _["colNames"] = colNames,
    _["upperIndex"] = upperIndex,
    _["lowerIndex"] = lowerIndex,
    _["res"] = res);
}

