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


#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>
#include <memory>

#include <Rcpp.h>

#include <interface/fstdefines.h>
#include <interface/istringwriter.h>
#include <interface/ifsttable.h>
#include <interface/icolumnfactory.h>
#include <interface/fststore.h>

#include <fst_error.h>
#include <fst_table.h>
#include <fst_column.h>
#include <fst_column_factory.h>

#include <flex_store.h>
#include <flex_store_v1.h>


using namespace std;
using namespace Rcpp;


inline int FindKey(StringVector colNameList, String item)
{
  int index = -1;
  int found = 0;
  for (Rcpp::StringVector::iterator it = colNameList.begin(); it != colNameList.end(); ++it)
  {
    if (*it == item)
    {
      index = found;
      break;
    }
    ++found;
  }

  return index;
}


SEXP fststore(String fileName, SEXP table, SEXP compression, SEXP uniformEncoding)
{
  if (!Rf_isLogical(uniformEncoding))
  {
    return fst_error("Parameter uniform.encoding should be a logical value");
  }

  if (!Rf_isInteger(compression))
  {
    return fst_error("Parameter compression should be an integer value between 0 and 100");
  }

  int compress = *INTEGER(compression);
  if ((compress < 0) | (compress > 100))
  {
    return fst_error("Parameter compression should be an integer value between 0 and 100");
  }

  FstTable fstTable(table, *LOGICAL(uniformEncoding));
  FstStore fstStore(fileName.get_cstring());

  try
  {
    fstStore.fstWrite(fstTable, compress);
  }
  catch (const std::runtime_error& e)
  {
    return fst_error(e.what());
  }

  return R_NilValue;
}


SEXP fstmetadata(String fileName, SEXP oldFormat)
{
  FstStore fstStore(fileName.get_cstring());
  std::unique_ptr<IColumnFactory> columnFactory(new ColumnFactory());

  // Character vector case 1:   2 2
  // Integer vector case 2:     4 5
  // Real vector case 3:        5 10
  // Logical vector case 4:     6 15
  // Factor vector case 5:      3 3

  // use fst format v0.7.2
  if (*LOGICAL(oldFormat) != 0)
  {
    try
    {
      List resOld = fstMeta_v1(fileName);  // scans further for safety

      IntegerVector typeVec = resOld[4];
      IntegerVector attributeVec(typeVec.size());

      // change type ordering
      int types[6] { 0, 2, 4, 5, 6, 3};
      int attributeTypes[6] { 0, 2, 5, 10, 15, 3};

      for (int pos = 0; pos < typeVec.size(); pos++)
      {
        int oldType = typeVec[pos];
        typeVec[pos] = types[oldType];
        attributeVec[pos] = attributeTypes[oldType];
      }

      resOld[4] = typeVec;
      resOld.push_back(attributeVec, "colType");

      return resOld;  // scans further for safety
    }
    catch(...)
    {
      return fst_error("An unknown C++ error occured in the legacy fstlib library. Please rewrite your fst files using the latest version of the fst package.");
    }
  }

  // use fst format >= v0.8.0
  try
  {
    fstStore.fstMeta(columnFactory.get());

  }
  catch (const std::runtime_error& e)
  {
    // We may be looking at a fst v0.7.2 file format, this unsafe code will be removed later
    if (std::strcmp(e.what(), FSTERROR_NON_FST_FILE) == 0)
    {
      return fst_error("File header information does not contain the fst format marker. "
        "If this is a fst file generated with package version older than v0.8.0, "
        "you can read your file by using 'old_format = TRUE'.");
    }

    return fst_error(e.what());
  }
  catch(...)
  {
    return fst_error("An unknown C++ error occured in the fstlib library.");
  }

  // R internals part TODO: speed up this code
  SEXP colNames = ((BlockReaderChar*) fstStore.blockReader)->StrVector();

  // Convert column info to integer vector
  // IntegerVector colTypeVec(fstStore.nrOfCols);
  IntegerVector colBaseType(fstStore.nrOfCols);
  IntegerVector colAttributeTypes(fstStore.nrOfCols);

  for (int col = 0; col != fstStore.nrOfCols; ++col)
  {
    // colTypeVec[col] = fstStore.colTypes[col];
    colBaseType[col] = fstStore.colBaseTypes[col];
    colAttributeTypes[col] = fstStore.colAttributeTypes[col];
  }

  List retList;

  if (fstStore.keyLength > 0)
  {
    SEXP keyNames;
    PROTECT(keyNames = Rf_allocVector(STRSXP, fstStore.keyLength));
    for (int i = 0; i < fstStore.keyLength; ++i)
    {
      SET_STRING_ELT(keyNames, i, STRING_ELT(colNames, fstStore.keyColPos[i]));
    }

    IntegerVector keyColIndex(fstStore.keyLength);
    for (int col = 0; col != fstStore.keyLength; ++col)
    {
      keyColIndex[col] = fstStore.keyColPos[col];
    }

    UNPROTECT(1);  // keyNames

    retList = List::create(
      _["nNofCols"]         = fstStore.nrOfCols,
      _["nrOfRows"]         = *(fstStore.p_nrOfRows),
      _["fstVersion"]       = fstStore.tableVersionMax,
      _["keyLength"]        = fstStore.keyLength,
      _["colBaseType"]      = colBaseType,
      _["colType"]          = colAttributeTypes,
      _["colNames"]         = colNames,
      _["keyColIndex"]      = keyColIndex,
      _["keyNames"]         = keyNames);
  }
  else
  {
    retList = List::create(
      _["nrOfCols"]        = fstStore.nrOfCols,
      _["nrOfRows"]        = *fstStore.p_nrOfRows,
      _["fstVersion"]      = fstStore.tableVersionMax,
      _["keyLength"]       = fstStore.keyLength,
      _["colBaseType"]     = colBaseType,
      _["colType"]         = colAttributeTypes,
      _["colNames"]        = colNames);
  }

  return retList;
}


SEXP fstretrieve(String fileName, SEXP columnSelection, SEXP startRow, SEXP endRow, SEXP oldFormat)
{
  FstTable tableReader;
  std::unique_ptr<IColumnFactory> columnFactory(new ColumnFactory());
  FstStore fstStore(fileName.get_cstring());

  int sRow = *INTEGER(startRow);

  // Set to last row
  int eRow = -1;

  if (!Rf_isNull(endRow))
  {
    eRow = *INTEGER(endRow);
  }

  vector<int> keyIndex;

  std::unique_ptr<StringArray> colNames(new StringArray());
  std::unique_ptr<StringArray> colSelection;

  if (!Rf_isNull(columnSelection))
  {
    colSelection = std::unique_ptr<StringArray>(new StringArray());
    colSelection->SetArray(columnSelection);
  }

  // use fst format v0.7.2
  if (*LOGICAL(oldFormat) != 0)
  {
    try
    {
      SEXP res = fstRead_v1(fileName, columnSelection, startRow, endRow);

      return res;
    }
    catch(...)
    {
      return fst_error("An unknown C++ error occured in the legacy fstlib library. "
        "Please rewrite your fst files using the latest version of the fst package.");
    }
  }

  // use fst format >= v0.8.0
  try
  {
    fstStore.fstRead(tableReader, colSelection.get(), sRow, eRow, columnFactory.get(), keyIndex, &*colNames);
  }
  catch (const std::runtime_error& e)
  {
    // We may be looking at a fst v0.7.2 file format, this unsafe code will be removed later
    if (std::strcmp(e.what(), FSTERROR_NON_FST_FILE) == 0)
    {
      return fst_error("File header information does not contain the fst format marker. "
        "If this is a fst file generated with package version older than v0.8.0, "
        "you can read your file by using 'old_format = TRUE'.");
    }

    // re-throw uncatched errors
    return fst_error(e.what());
  }
  catch(...)
  {
    return fst_error("An unknown C++ error occured in the fstlib library");
  }

  SEXP colNameVec = colNames->StrVector();

  // Generalize to full atributes
  Rf_setAttrib(tableReader.resTable, R_NamesSymbol, colNameVec);


  // Convert keyIndex to keyNames
  SEXP keyNames;
  PROTECT(keyNames = Rf_allocVector(STRSXP, keyIndex.size()));

  int count = 0;

  for (vector<int>::iterator keyIt = keyIndex.begin(); keyIt != keyIndex.end(); ++keyIt)
  {
    SET_STRING_ELT(keyNames, count++, STRING_ELT(colNameVec, *keyIt));
  }

  UNPROTECT(1);

  return List::create(
    _["keyNames"]   = keyNames,
    _["keyIndex"]   = keyIndex,
    _["colNameVec"] = colNameVec,
    _["resTable"]   = tableReader.resTable);
}
