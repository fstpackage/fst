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


#include <Rcpp.h>
#include <Rcpp.h>


inline void fst_quicksort(int* vec, int length) {

  int pos_left = 0;
  int pos_right = length - 1;

  // take center value as median estimate
  int pivot = (vec[0] + vec[pos_right]) / 2;

  while (TRUE) {
    // iterate left until value > pivot
    while (pos_left < pos_right && vec[pos_left] <= pivot) pos_left++;

    // left swap value found, iterate right until value < pivot
    while (pos_right > pos_left && vec[pos_right] > pivot) pos_right--;

    if (pos_left == pos_right) break;

    // swap values and restart
    int tmp = vec[pos_right];
    vec[pos_right] = vec[pos_left];
    vec[pos_left] = tmp;
  }

  // pos_left == pos_right as this point

  if (vec[pos_left] < pivot) {
    pos_left++;
  }

  if (pos_left > 2) {
    fst_quicksort(vec, pos_left);
  }
  else if (pos_left == 2 && vec[0] > vec[1]) {
  // swap first 2 elements
    int tmp = vec[1];
    vec[1] = vec[0];
    vec[0] = tmp;
  }

  if (pos_left < (length - 2)) {
    fst_quicksort(&vec[pos_left], length - pos_left);
  }

  // swap last 2 elements if in reverse order
  if (pos_left == (length - 2) && vec[pos_left] > vec[pos_left + 1]) {
    int tmp = vec[pos_left];
    vec[pos_left] = vec[pos_left + 1];
    vec[pos_left + 1] = tmp;
  }
}


SEXP fstsort(SEXP int_vec) {

  int* vec = INTEGER(int_vec);
  int length = LENGTH(int_vec);

  if (length == 0) return int_vec;

  // split in two sorted vectors
  fst_quicksort(vec, length);
  return int_vec;
}
