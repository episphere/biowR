#' Mangles the filename to prevent name collision
#'
#' The file names are mangled by insert the date-time right before the file extension.
#' Because of this, all file names are required to have at least a one letter extension.
#'
#' @param x A filename containing a file extension of at least 1 character.
#' The argument x can be a vector of filenames. For mangle_filename, although each value
#' must be unique or else you could have two files with the same name.
#' Best practice is to make x simply a string.  For demangle_filename, using a vector
#' causes no problems
#'
#' @return the vector of mangeled or demangled filenames
#' @export
#'
#' @examples
#' # create a vector of files...
#' files <- c("~/file1.txt","~/file2.txt")
#' mangled <- mangle_filename(files)
#' demangled <- demangle_filename(mangled)
#' # check that the demangle files are the same as the original files.
#' all(demangled == files)
mangle_filename <- function(x){
  sub("(\\.[^\\.]*)$",paste0("_",format(Sys.time(),"%Y%m%d%H%M%OS4"),"\\1"),x)
}

#' @rdname mangle_filename
#' @export
demangle_filename <- function(x){
  sub("_[0-9\\.]+(\\.[^\\.]*$)","\\1",x)
}

#'
#' Stage files from one dir to another.
#'
#' On the biowulf system, the data disk is very slow for constant IO.  You
#' will want to stage the data from the data disk to the local scratch disk.
#'
#' @param fromDir directory you are copying data from
#' @param toDir  directory where the data is going to
#' @param mangle should the filenames be mangled while staging the files?
#' @param pattern (optional) pattern used to select files to stage see [list.files()]
#' @param f0 (optional) the starting file index, if you only want to copy a
#' subset of the file (e.g. from file 3 to file 7).  By default is 0.
#' @param f1 (optional) the ending file index, if you only want to copy a
#' subset of the file.
#'
#' @export
#'
stage_files <- function(fromDir,toDir,mangle=FALSE,pattern,f0=0,f1){
  if (!dir.exists(fromDir)) stop("directory ",fromDir," does not exist")
  if (!dir.exists(toDir)) stop("directory ",toDir," does not exist")
#  if (!missing(pattern) && !missing(f1)){
#    stop("either pattern or f1 can be given")
#  }

  if (missing(pattern)){
    fromFiles <- dir(fromDir,full.names = TRUE)
  }else{
    fromFiles <- dir(fromDir,pattern=pattern,full.names = TRUE)
  }
  toFiles <- sub(fromDir,toDir,fromFiles)

  if (mangle){
    toFiles <- mangle_filename(toFiles)
  }

  if (missing(f1)){
    file.copy(fromFiles,toFiles)
  }else{
    file.copy(fromFiles[f0:f1],toFiles[f0:f1])
  }
}

#' Stage files from a list of files
#'
#' Given a vector of filenames in the directory fromDir, stage the files into toDir.
#'
#' @param fromDir  the directory where all the files are located, all files must be in the same directory.
#' @param toDir    where the files are copied.
#' @param filelist The list of files
#' @param f0       The index of the first file in the list to be copied (default = 1)
#' @param f1       The index of the last file in the list to be copied (default = length(filelist))
#'
#' @export
#'
stage_filelist<-function(fromDir,toDir,filelist,f0=1,f1=length(filelist)){
  if (!dir.exists(fromDir)) stop("directory ",fromDir," does not exist")
  if (!dir.exists(toDir)) stop("directory ",toDir," does not exist")

  allfiles <- dir(fromDir)
  filelist<-basename(filelist)
  ## make sure all the files exist...
  not_in_fromdir = setdiff(filelist,allfiles)
  filelist <- intersect(filelist,allfiles)
  if (length(not_in_fromdir)>0){
    warning("some of the file where not in the input directory: ",not_in_fromdir)
  }
  rm(not_in_fromdir)

  fromFiles <- file.path(fromDir,filelist)
  toFiles <- file.path(toDir,filelist)

  file.copy(fromFiles[f0:f1],toFiles[f0:f1])
}



#' Concatenates files that would have the name if they were not mangled
#'
#' Takes the unmangled name and finds all files that would be mangled to that file and
#' concatentes them all together.
#'
#' @param fileDir    directory containing all the mangled files
#' @param unmangled  the unmangled filename.
#'
#'
#' @export
#'
concat_mangled_files <- function(fileDir,unmangled){
  handle_file <-function(x){
    tryCatch({
      readr::read_csv(x,show_col_types=FALSE) %>% readr::write_csv(file = unmangled,append = file.exists(unmangled))
      unlink(x)
    },
    error = function(ex){
      warning("caught an exception: ",ex," file: ",x)
    })
  }

  if (unmangled == basename(unmangled)) unmangled <- file.path(fileDir,unmangled)

  files <- dir(fileDir,full.names = TRUE)
  files %>%
    purrr::discard(~.x == unmangled) %>%
    purrr::keep(~demangle_filename(.x) == unmangled) %>%
    purrr::walk(handle_file)
}

## runs slower than without futures???
concat_mangled_files_future <- function(fileDir,unmangled){

  future::plan(future::multisession)
  handle_file <- function(last_future,filename){
    data <- readr::read_csv(filename,show_col_types=FALSE)
    # because the last_future may not be resolved,
    # block until it is complete..
    future::value(last_future)
    # excellent we are ready to continue
    next_future <- future::future({
      readr::write_csv(data,unmangled,append = file.exists(unmangled),progress = FALSE)
      unlink(filename)
    })
  }

  if (unmangled == basename(unmangled)) unmangled <- file.path(fileDir,unmangled)

  files <- dir(fileDir,full.names = TRUE)
  files %>%
    purrr::discard(~.x == unmangled) %>%
    purrr::keep(~demangle_filename(.x) == unmangled) %>%
    purrr::reduce(handle_file,.init=future::future(TRUE)) %>% future::value(.)
}
