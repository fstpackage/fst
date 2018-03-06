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

#include <algorithm>

#ifdef _OPENMP
#include <omp.h>
#endif

#include "openmphelper.h"


static int FstThreads = 0;

int GetFstThreads()
{
#ifdef _OPENMP
  int ans = FstThreads == 0 ? omp_get_max_threads() : std::min(FstThreads, omp_get_max_threads());
  return std::max(1, ans);
#else
  return 1;
#endif
}


int ThreadsFst(int nrOfThreads)
{
  int oldNrOfThreads = GetFstThreads();
  FstThreads = nrOfThreads;
  return oldNrOfThreads;
}


int CurrentFstThread()
{
#ifdef _OPENMP
  return omp_get_thread_num();
#else
  return 0;
#endif
}


void SetThreads(int nrOfThreads)
{
  FstThreads = nrOfThreads;
}


int GetThreads()
{
  return FstThreads;
}


bool HasOpenMP()
{
#ifdef _OPENMP
  return true;
#else
  return false;
#endif
}
