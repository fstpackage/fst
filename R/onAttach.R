
.onAttach <- function(libname, pkgname)
{
  # Runs when attached to search() path such as by library() or require()
  if (interactive())
  {
    v = packageVersion("fst")
    d = read.dcf(system.file("DESCRIPTION", package="fst"), fields = c("Packaged", "Built"))

    if(is.na(d[1]))
    {
      if(is.na(d[2]))
      {
        return() #neither field exists
      } else
      {
        d = unlist(strsplit(d[2], split="; "))[3]
      }
    } else
    {
      d = d[1]
    }

    # version number odd => dev
    dev = as.integer(v[1,3])%%2 == 1

    packageStartupMessage("fst ", v, if(dev) paste0(" IN DEVELOPMENT built ", d))

    # Check for old version
    if (dev && (Sys.Date() - as.Date(d)) > 28)
        packageStartupMessage("\n!!! This development version of the package is rather old, please update !!!")

    # Check for OpenMP support
    if (!hasOpenMP())
    {
      packageStartupMessage("(OpenMP was not detected, using single threaded mode)")
    } else
    {
      packageStartupMessage("(OpenMP detected, ", getDTthreads(), " threads activated)")
    }
  }
}

