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

#include <interface/openmphelper.h>

#ifdef _OPENMP
#include <pthread.h>
#endif

SEXP getnrofthreads()
{
    return Rf_ScalarInteger(GetFstThreads());
}


int setnrofthreads(SEXP nrOfThreads)
{
    SEXP intVal = Rf_coerceVector(nrOfThreads, INTSXP);

    if (!Rf_isInteger(intVal) || Rf_length(intVal) != 1 || INTEGER(intVal)[0] < 0)
    {
        // catches NA too since NA is -ve
        Rf_error("Argument to threads_fst must be a single integer >= 0. \
            Default 0 is recommended to use all CPU.");
    }

    return SetFstThreads(*INTEGER(intVal));
}


// Folowing code was adopted from package data.table:
// TODO: implement in fstcore to guard against forking issues with OpenMP

void when_fork()
{
    SetFstThreads(1);
}


extern "C" int avoid_openmp_hang_within_fork()
{
    // Called once on loading fst from init.c
#ifdef _OPENMP
    return pthread_atfork(&when_fork, NULL, NULL);
#endif

    return 0;
}


SEXP hasopenmp()
{
  return Rf_ScalarLogical(HasOpenMP());
}

