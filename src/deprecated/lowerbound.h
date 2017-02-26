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

#ifndef LOWERBOUND_H
#define LOWERBOUND_H

#include <Rcpp.h>
#include <R.h>
#include <Rinternals.h>

using namespace Rcpp;
using namespace std;


// When calling, use 'lower' 1 smaller than the first element that you want to check. That way, when the result == lower - 1,
// you know that all elements are larger than the key.
int lastIntEqualLower(int* intVec, int key, int lower, int upper);

// When calling, use 'upper' 1 larger than the last element you want to check. That way, when result == upper,
// you know that all elements are smaller than the key.
int firstIntEqualHigher(int* intVec, int key, int lower, int upper);

int lastDoubleEqualLower(double* doubleVec, double key, int lower, int upper);

int firstDoubleEqualHigher(double* doubleVec, double key, int lower, int upper);

int lastCharEqualLower(SEXP charVec, const char* key, int lower, int upper);

int firstCharEqualHigher(SEXP charVec, const char* key, int lower, int upper);

#endif // LOWERBOUND_H
