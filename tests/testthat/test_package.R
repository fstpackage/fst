
context("package")

# update commit key to latest commit
if (file.exist("keyupdater")) {
  if (require(git2r)) {

    last_commit <- git2r::commits()[[1]]
    last_hash <- substr(last_commit@sha, 1, 7)

    onAttach_file <- readLines("../../R/onAttach.R")
    first_match <- grep("if (interactive())", onAttach_file, fixed = TRUE)

    if (length(first_match) == 0) break

    if (grep("commit_hash <- ", onAttach_file[first_match[1] + 1], fixed = TRUE) != 1) break

    onAttach_file[first_match[1] + 1] <- paste0("    commit_hash <- \"", last_hash, "\"")

    writeLines(onAttach_file, "../../R/onAttach.R")
  }
}
