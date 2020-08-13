#include <R.h>
#include <Rinternals.h>
#include <stdlib.h> // for NULL
#include <R_ext/Rdynload.h>

/* FIXME: 
   Check these declarations against the C/Fortran source code.
*/

/* .Call calls */
extern SEXP mapi_split(SEXP, SEXP);

static const R_CallMethodDef CallEntries[] = {
    {"mapi_split", (DL_FUNC) &mapi_split, 2},
    {NULL, NULL, 0}
};

void R_init_MonetDB_R(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
