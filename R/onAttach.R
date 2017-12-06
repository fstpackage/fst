#  fst - R package for ultra fast storage and retrieval of datasets
#
#  Copyright (C) 2017-present, Mark AJ Klik
#
#  This file is part of the fst R package.
#
#  The fst R package is free software: you can redistribute it and/or modify it
#  under the terms of the GNU Affero General Public License version 3 as
#  published by the Free Software Foundation.
#
#  The fst R package is distributed in the hope that it will be useful, but
#  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#  FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
#  for more details.
#
#  You should have received a copy of the GNU Affero General Public License along
#  with the fst R package. If not, see <http://www.gnu.org/licenses/>.
#
#  You can contact the author at:
#  - fst R package source repository : https://github.com/fstpackage/fst


.onAttach <- function(libname, pkgname) {
  # Runs when attached to search() path such as by library() or require()
  if (interactive()) {
    commit_hash <- "26fef67"
    v <- packageVersion("fst")
    d <- read.dcf(system.file("DESCRIPTION", package = "fst"), fields = c("Packaged", "Built"))

    if (is.na(d[1])) {
      if (is.na(d[2])) {
        return() # neither field exists
      } else {
        d <- unlist(strsplit(d[2], split = "; "))[3]
      }
    } else {
      d <- d[1]
    }

    # version number odd => dev
    dev <- as.integer(v[1, 3]) %% 2 == 1

    packageStartupMessage("fst package v", v,
      if (dev) paste0(" IN DEVELOPMENT build ", commit_hash, " (", d, ")"))

    # Check for old version
    if (dev && (Sys.Date() - as.Date(d)) > 28)
        packageStartupMessage("\n!!! This development version of the package is rather old, please update !!!")

    # Check for OpenMP support
    if (!hasopenmp()) {
      packageStartupMessage("(OpenMP was not detected, using single threaded mode)")
    } else {
      # Use only physical cores to maximize performance (no hyperthreading)
      physical_cores <- parallel::detectCores(logical = FALSE)
      logical_cores <- parallel::detectCores(logical = TRUE)

      threads_fst(logical_cores)
      packageStartupMessage("(OpenMP detected, using ", threads_fst(),
        if (physical_cores != logical_cores) " logical", " cores)")
    }
  }
}
