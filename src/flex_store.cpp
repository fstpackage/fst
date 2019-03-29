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
#include <fst_string_vector_container.h>

#include <flex_store.h>


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


int fstlib_version()
{
  return FST_VERSION_MAJOR * 64 * 64 + FST_VERSION_MINOR * 64 + FST_VERSION_RELEASE;
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

  // avoid using PROTECT statements in C++ classes (which generate rchk errors)
  // this PROTECTED container can be used to hold any R object safely
  SEXP r_container = PROTECT(Rf_allocVector(VECSXP, 1));

  FstTable fstTable(table, *LOGICAL(uniformEncoding), r_container);
  FstStore fstStore(fileName.get_cstring());

  try
  {
    fstStore.fstWrite(fstTable, compress);
  }
  catch (const std::runtime_error& e)
  {
    UNPROTECT(1);
    return fst_error(e.what());
  }

  UNPROTECT(1);
  return R_NilValue;
}


SEXP fstmetadata(String fileName)
{
  FstStore fstStore(fileName.get_cstring());
  std::unique_ptr<IColumnFactory> columnFactory(new ColumnFactory());

  // to hold the column names
  SEXP list_container = PROTECT(Rf_allocVector(VECSXP, 1));
  StringVectorContainer* str_container = new StringVectorContainer(list_container);
  std::unique_ptr<IStringColumn> col_names(str_container);

  // use fst format >= v0.8.0
  try
  {
    fstStore.fstMeta(columnFactory.get(), col_names.get());
  }
  catch (const std::runtime_error& e)
  {
    UNPROTECT(1); // list_container

    // We may be looking at a fst v0.7.2 file format, this unsafe code will be removed later
    if (std::strcmp(e.what(), FSTERROR_NON_FST_FILE) == 0)
    {
      return fst_error("File header information does not contain the fst format marker. "
        "If this is a fst file generated with package version older than v0.8.0, "
        "please read (and re-write) your file using fst package versions 0.8.0 to 0.8.10.");
    }

    return fst_error(e.what());
  }
  catch(...)
  {
    UNPROTECT(1); // list_container

    return fst_error("An unknown C++ error occured in the fstlib library.");
  }

  // R internals part TODO: speed up this code
  // SEXP colNames = ((BlockReaderChar*) fstStore.blockReader)->StrVector();
  SEXP colNames = str_container->StrVector();

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

    UNPROTECT(2);  // keyNames, list_container

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
    UNPROTECT(1); // list_container

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


SEXP fstretrieve(String fileName, SEXP columnSelection, SEXP startRow, SEXP endRow)
{
  // avoid using PROTECT statements in C++ classes (which generate rchk errors)
  // this PROTECTED container can be used to hold any R object safely
  SEXP r_container = PROTECT(Rf_allocVector(VECSXP, 1));

  // to hold the column names
  SEXP list_container = PROTECT(Rf_allocVector(VECSXP, 1));
  StringVectorContainer* str_container = new StringVectorContainer(list_container);
  std::unique_ptr<IStringColumn> col_names(str_container);

  FstTable tableReader(r_container);

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

  // use fst format >= v0.8.0
  std::unique_ptr<StringArray> colSelection;
  std::unique_ptr<StringArray> colNames(new StringArray());

  if (!Rf_isNull(columnSelection))
  {
    colSelection = std::unique_ptr<StringArray>(new StringArray());
    colSelection->SetArray(columnSelection);
  }

  try
  {
    fstStore.fstRead(tableReader, colSelection.get(), sRow, eRow, columnFactory.get(), keyIndex, &*colNames, col_names.get());
  }
  catch (const std::runtime_error& e)
  {
    // We may be looking at a fst v0.7.2 file format, this unsafe code will be removed later
    if (std::strcmp(e.what(), FSTERROR_NON_FST_FILE) == 0)
    {
      UNPROTECT(2);  // r_container, list_container
      return fst_error("File header information does not contain the fst format marker. "
        "If this is a fst file generated with package version older than v0.8.0, "
        "please read (and re-write) your file using fst package versions 0.8.0 to 0.8.10.");
    }

    // re-throw uncatched errors
    UNPROTECT(2);  // r_container, list_container
    return fst_error(e.what());
  }
  catch(...)
  {
    UNPROTECT(2);  // r_container, list_container
    return fst_error("An unknown C++ error occured in the fstlib library");
  }

  SEXP colNameVec = tableReader.GetColNames();

  // SEXP colNameVec = colNames->StrVector();

  // Generalize to full atributes
  SEXP resTable = PROTECT(tableReader.ResTable());
  Rf_setAttrib(resTable, R_NamesSymbol, colNameVec);
  UNPROTECT(1);

  // Convert keyIndex to keyNames
  SEXP keyNames = PROTECT(Rf_allocVector(STRSXP, keyIndex.size()));

  int count = 0;

  for (vector<int>::iterator keyIt = keyIndex.begin(); keyIt != keyIndex.end(); ++keyIt)
  {
    SET_STRING_ELT(keyNames, count++, STRING_ELT(colNameVec, *keyIt));
  }

  UNPROTECT(3);  // r_container, keyNames, list_container

  return List::create(
    _["keyNames"]   = keyNames,
    _["keyIndex"]   = keyIndex,
    _["resTable"]   = tableReader.ResTable());
}
