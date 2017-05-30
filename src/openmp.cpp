
#include <Rcpp.h>

#include <interface/openmphelper.h>

// #ifdef _OPENMP
//   #include <omp.h>
// #else // so it still compiles on machines with compilers void of openmp support
//   #define omp_get_num_threads() 1
//   #define omp_get_thread_num() 0
// #endif

#ifdef _OPENMP
#include <pthread.h>
#endif

/* GOALS:
* 1) By default use all CPU for end-user convenience in most usage scenarios.
* 2) But not on CRAN - two threads max is policy
* 3) And not if user doesn't want to:
*    i) Respect env variable OMP_NUM_THREADS (which just calls (ii) on startup)
*    ii) Respect omp_set_num_threads()
*    iii) Provide way to restrict data.table only independently of base R and
*         other packages using openMP
* 4) Avoid user needing to remember to unset this control after their use of data.table
* 5) Automatically drop down to 1 thread when called from parallel package (e.g. mclapply) to
*    avoid the deadlock/hang (#1745 and #1727) and return to prior state afterwards.
*/



SEXP getNrOfActiveThreads()
{
    return Rf_ScalarInteger(GetFstThreads());
}


int setNrOfActiveThreads(SEXP nrOfThreads)
{
    SEXP intVal = Rf_coerceVector(nrOfThreads, INTSXP);

    if (!Rf_isInteger(intVal) || Rf_length(intVal) != 1 || INTEGER(intVal)[0] < 0)
    {
        // catches NA too since NA is -ve
        Rf_error("Argument to setNrOfActiveThreads must be a single integer >= 0. \
            Default 0 is recommended to use all CPU.");
    }

    return SetFstThreads(*INTEGER(intVal));
}


//
// SEXP setDTthreads(SEXP threads) {
//     if (!isInteger(threads) || length(threads) != 1 || INTEGER(threads)[0] < 0) {
//         // catches NA too since NA is -ve
//         error("Argument to setDTthreads must be a single integer >= 0. Default 0 is recommended to use all CPU.");
//     }
//     // do not call omp_set_num_threads() here as that affects other openMP
//     // packages and base R as well potentially.
//     int old = DTthreads;
//     DTthreads = INTEGER(threads)[0];
//     return ScalarInteger(old);
// }

// auto avoid deadlock when fst called from parallel::mclapply
static int preFork_Fstthreads = 0;

void when_fork() {
    preFork_Fstthreads = GetFstThreads();
    SetFstThreads(1);
}

void when_fork_end() {
    SetFstThreads(preFork_Fstthreads);
}


int avoid_openmp_hang_within_fork()
{
    // Called once on loading fst from init.c
#ifdef _OPENMP
    return pthread_atfork(&when_fork, &when_fork_end, nullptr);
#endif

    return 0;
}


SEXP hasOpenMP()
{
  return Rf_ScalarLogical(HasOpenMP());
}

