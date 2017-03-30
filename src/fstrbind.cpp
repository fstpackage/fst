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

#include <fstream>

#include <fstrbind.h>
#include <fstmetadata.h>
#include <fstdefines.h>
#include <fsttable.h>
#include <fststore.h>


using namespace Rcpp;
using namespace std;


// [[Rcpp::export]]
SEXP BytesConvert(SEXP integer)
{
  int* intVal = INTEGER(integer);
  unsigned int uVal = (unsigned int) intVal[0];

  vector<int> result(32);
  vector<int> resultU(32);

  for (int count = 0; count < 32; ++count)
  {
    result[count] = (intVal[0] >> (31 - count)) & 1;
    resultU[count] = (uVal >> (31 - count)) & 1;
  }

  int toInt= (int) uVal;

  return List::create(
    _["bytes"] = result,
    _["bytesU"] = resultU,
    _["uval"] = uVal,
    _["toInt"] = toInt);
}


SEXP fstrbind(String fileName, SEXP table, SEXP compression, Function serializer)
{
  SEXP colNames = Rf_getAttrib(table, R_NamesSymbol);
  SEXP keyNames = Rf_getAttrib(table, Rf_mkString("sorted"));
  int nrOfCols = LENGTH(colNames);


  // Assume a data.table


  // Create FstTable proxy object

  SEXP firstColumn = VECTOR_ELT(table, 0);  // column vector
  uint64_t nrOfRows = Rf_xlength(firstColumn);
  uint64_t nrOfRows = Rf_xlength(firstColumn);


  // Create a FstTable wrapper around an R data frame
  FstTable fstTable(nrOfRows);

  // Add FstColumns
  for (int colNr = 0; colNr < nrOfCols; ++colNr)
  {
    SEXP colVec = VECTOR_ELT(table, colNr);  // column vector

    switch (TYPEOF(colVec))
    {
      case INTSXP:
        if(!Rf_isFactor(colVec))
        {
          fstTable.AddColumnInt32(INTEGER(colVec));

          break;
        }

      default:
        ::Rf_error("Column type not implemented in rbind yet.");
    }
  }


  // Access to fst file
  FstStore fstStore(fileName, true);

  try
  {
    fstStore.ColBind(fstTable);
  }
  catch (std::exception &e)
  {
    // caught exeption
    ::Rf_error(e.what());
  }


  return List::create(
    _["colNames"] = colNames,
    _["keyNames"] = keyNames);
}

