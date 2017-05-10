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

#include "attributes.h"
#include <fstream>

using namespace std;
using namespace Rcpp;


// In general, attributes contain a small amount of data. Therefore, we can use R's internal serialization mechanism
// to stream the attributes (we can optimize known types in a future release if speed is too low).
inline unsigned int SerializeObjectAttributes(ofstream &myfile, RObject rObject, Function serializer)
{
  vector<string> attrNames = rObject.attributeNames();
  unsigned int nrOfAttributes = attrNames.size();

  if (nrOfAttributes == 0) return 0;

  // Create a list with attribute data for serialization
  List outList(nrOfAttributes + 1);

  int listIndex = 0;
  for (std::vector<string>::iterator it = attrNames.begin() ; it != attrNames.end(); ++it)
  {
    SEXP attribute = rObject.attr(*it);
    outList[listIndex++] = attribute;
  }

  outList[nrOfAttributes] = attrNames;

  RawVector serializedAttributes = serializer(outList, R_NilValue);

  unsigned int dataLength =  LENGTH(serializedAttributes);
  myfile.write((char*) RAW(serializedAttributes), dataLength);

  return dataLength;
}


inline SEXP UnserializeRaw(unsigned char* rawData, unsigned int rawLength, Function unserializer)
{
  RawVector rawVec(rawLength);
  memcpy(RAW(rawVec), rawData, rawLength);
  return unserializer(rawVec);
}


SEXP UnserializeObjectAttributes(SEXP rObject, RawVector rawVector, Function unserializer)
{
  SEXP attributes = UnserializeRaw(RAW(rawVector), LENGTH(rawVector), unserializer);

  int nrOfAttributes = LENGTH(attributes) - 1;
  SEXP attrNames = VECTOR_ELT(attributes, nrOfAttributes);

  for (int itemNr = 0; itemNr != nrOfAttributes; ++itemNr)
  {
    Rf_setAttrib(rObject, Rf_mkString(CHAR(STRING_ELT(attrNames, itemNr))), VECTOR_ELT(attributes, itemNr));
  }

  SEXP a = STRING_ELT(attrNames, 0);
  SEXP b = STRING_ELT(attrNames, 1);

  return List::create(
    _["attributes"] = attributes,
    _["attrNames"] = attrNames,
    _["nrOfAttributes"] = nrOfAttributes,
    _["rObject"] = rObject,
    _["a"] = a,
    _["b"] = b);
}


