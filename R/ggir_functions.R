#' Stage results from GGIR part 1
#'
#' @description
#' The way we run GGIR steps 2-5 on biowulf is to copy a set of results
#' from step 1 to a local staging scratch directory on one of the nodes.
#' The scratch directory is only available on the biowulf compute node.
#' Then steps 2-5 are run.
#'
#' This function takes the results from step 1 and copies them on a local
#' scratch disk on a biowulf compute node.
#'
#' @note The output directory is **output_studyname**
#'
#' @param output_dir The output directory of the results
#' @param f0 The index of the file in the directory (starting with 1)
#' @param f1 The index of the last file in the directory
#'
#' @return invisibly returns the stage output directory
#' @export
#'
stage_part1_results <- function(output_dir,f0,f1){
  if (!file.exists(output_dir)) stop(output_dir," does not exist")
  stage_output <- fs::dir_create(get_scratch_dir(),"stage", basename(output_dir))
  #
  # Build the directories in stage...
  #
  fs::dir_create(stage_output,"results",c("file summary reports","QC"))
  to_dir <- fs::dir_create(stage_output,"meta","basic")

  #
  # copy the files...
  #
  from_dir = file.path(output_dir,"meta","basic")

  f0 = as.integer(f0)
  files <- list.files(from_dir)
  if (f0>length(files)) stop("f0 (",f0,") is larger than the number of files (",length(files),"). ")
  if (f0<1) stop("f0 (",f0,") is less than 1")
  if (f1==0 || f1>length(files)) f1=length(files)
  if (f1<f0) stop("f1 (",f1,") is less than f0 (",f0,")")


  ok <- file.copy(file.path(from_dir,files[f0:f1]),to_dir)
  if (!all(ok)){
    message("trouble staging files:")
    print(files[f0:f1][!ok])
  }
  invisible(stage_output)
}


#' @rdname runggir
#' @order 2
#' @export
run_stages_2_5 <- function(part1_output_dir,json_args="",f0,f1){
  stopifnot(startsWith(basename(part1_output_dir),prefix = "output_"))

  # stage the results from part1
  stage_output_dir <- stage_part1_results(part1_output_dir,f0,f1)

  #
  # get GGIR params from the json file ...
  #
  args=list()
  if (file.exists(json_args)){
    args <- jsonlite::read_json(json_args,simplifyVector = T)
  }
  # set or override some of the arguments...
  args$mode=2:5
  args$outputdir=dirname(stage_output_dir)
  args$studyname = gsub("output_","",basename(part1_output_dir))
  args$datadir = file.path(dirname(stage_output_dir),"accelerometer")
  message("FORCING do.part2.pdf,do.part3.pdf,do.visual to FALSE")
  args$do.part2.pdf = FALSE
  args$do.part3.pdf = FALSE
  args$do.visual = FALSE

  #
  # run GGIR
  #
  do.call(GGIR::GGIR,args)
  message("unstaging results")
  file_info <- unstage_all(stage_output_dir,part1_output_dir)
  unlink(stage_output_dir,recursive = T)
}


#' Copy results from the staging back to the results directory
#'
#' @description
#' The way we run GGIR steps 2-5 on biowulf is to copy a set of results
#' from step 1 to a local staging scratch directory on one of the nodes.
#' The scratch directory is only available on the compute node.
#' Then steps 2-5 are run.
#'
#' This function takes the results from steps 2-5 and returns it to the
#' central results directory, which is a networked disk.
#'
#' The output directory is named **output_studyname**.
#'
#' @param stage_output the output directory on the stage disk
#' @param results_output the central output director
#'
#' @return a tibble with columns (file,name,relpath,mangle,ok)
#
#'
#'| **file** | **name** | **relpath** | **mangle** | **ok** |
#'| --- | --- | --- | --- | --- |
#'| relative path of the files | basename of the file. | relative name of the files' directory | Was the file mangled? | Did the file copies ok? |
#'
#' @export
#' @importFrom rlang .data
unstage_all<-function(stage_output,results_output){
  fc<-function(file,mangle=FALSE){
    from=file.path(stage_output,file)
    to=file.path(results_output,file)
    if (mangle) to=mangle_filename(to)
    fs::dir_create(dirname(to))
    file.copy(from = from,to=to)
  }

  fileInfo <- tibble::tibble(
    file=setdiff(dir(stage_output,include.dirs = F,recursive = T),dir(results_output,include.dirs = F,recursive = T)),
    name=basename(file),
    relpath=dirname(file),
    mangle=!grepl("^\\.|meta|file summary report",.data$relpath)
  )

  fileInfo %>% dplyr::mutate(ok=purrr::map2_lgl(.data$file,.data$mangle,fc))
}
