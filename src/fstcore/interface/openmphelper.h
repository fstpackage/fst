/*
 fstlib - A C++ library for ultra fast storage and retrieval of datasets

Copyright (C) 2017-present, Mark AJ Klik

This file is part of fstlib.

fstlib is free software: you can redistribute it and/or modify it under the
terms of the GNU Affero General Public License version 3 as published by the
Free Software Foundation.

fstlib is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
details.

You should have received a copy of the GNU Affero General Public License
along with fstlib. If not, see <http://www.gnu.org/licenses/>.

You can contact the author at:
- fstlib source repository : https://github.com/fstpackage/fstlib
*/


#ifndef OPEN_MP_HELPER_H
#define OPEN_MP_HELPER_H


#ifdef _OPENMP
#include <omp.h>
#endif


/**
* \brief Get the number of threads used in parallel computations.
* \return number of threads used.
*/
int GetFstThreads();


/**
 * \brief Set the number of threads and query the current number of threads used.
 * \param nrOfThreads
 * \return number of threads to use for parallel computation.
 */
int ThreadsFst(int nrOfThreads);


/**
 * \brief Return the currently active thread in an OpenMP construct.
 * \return ID of the currently active thread.
 */
int CurrentFstThread();


/**
 * \brief Set the number of threads without querying the current number of threads used.
 * Use this method instead of ThreadsFst when calling from a fork.
 * \param nrOfThreads number of threads to use for parallel computation.
 */
void SetThreads(int nrOfThreads);


/**
 * \brief Get the number of threads without using the OpenMP API.

 * \return The number of threads currently set.
 */
int GetThreads();


/**
 * \brief Check if the library is compiled with OpenMP
 * \return Returns true if OpenMP is anabled, false otherwise.
 */
bool HasOpenMP();

#endif  // OPEN_MP_HELPER_H
