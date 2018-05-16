
#include <R.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL
#include <R_ext/Rdynload.h>

/* FIXME:
Check these declarations against the C/Fortran source code.
*/

/* .Call calls */
extern SEXP _fst_fstcomp(SEXP, SEXP, SEXP, SEXP);
extern SEXP _fst_fstdecomp(SEXP);
extern SEXP _fst_fsthasher(SEXP, SEXP, SEXP);
extern SEXP _fst_fstmetadata(SEXP, SEXP);
extern SEXP _fst_fstretrieve(SEXP, SEXP, SEXP, SEXP, SEXP);
extern SEXP _fst_fststore(SEXP, SEXP, SEXP, SEXP);
extern SEXP _fst_getnrofthreads();
extern SEXP _fst_hasopenmp();
extern SEXP _fst_setnrofthreads(SEXP);
extern SEXP _fst_restore_after_fork(SEXP);
extern int avoid_openmp_hang_within_fork();


static const R_CallMethodDef CallEntries[] = {
  {"_fst_fstcomp",            (DL_FUNC) &_fst_fstcomp,            4},
  {"_fst_fstdecomp",          (DL_FUNC) &_fst_fstdecomp,          1},
  {"_fst_fsthasher",          (DL_FUNC) &_fst_fsthasher,          3},
  {"_fst_fstmetadata",        (DL_FUNC) &_fst_fstmetadata,        2},
  {"_fst_fstretrieve",        (DL_FUNC) &_fst_fstretrieve,        5},
  {"_fst_fststore",           (DL_FUNC) &_fst_fststore,           4},
  {"_fst_getnrofthreads",     (DL_FUNC) &_fst_getnrofthreads,     0},
  {"_fst_hasopenmp",          (DL_FUNC) &_fst_hasopenmp,          0},
  {"_fst_restore_after_fork", (DL_FUNC) &_fst_restore_after_fork, 1},
  {"_fst_setnrofthreads",     (DL_FUNC) &_fst_setnrofthreads,     1},
  {NULL, NULL, 0}
};

void R_init_fst(DllInfo *dll)
{
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);

  avoid_openmp_hang_within_fork();  // don't use OpenMP after forking
}
