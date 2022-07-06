#' Run Stage 1 of GGIR on the NIH biowulf.
#'
#' On the NIH biowulf, reading files directly from the /data disk is EXTREMELY slow.  So
#' we stage the data, files [f0..f1)  on local disk and run all the local files.
#'
#' The goal is to make a pipeline that is swarmable.  Nothing is returned by
#' this function, however, files are written in the results root.
#'
#' The output of this should would with the input of run_stage_2_5.
#'
#' @param cwa_root where the CWA files are stored.
#' @param results_root where to write the results
#' @param json_args a json file containing a list of arguments passed into ggir stage 1
#' @param f0 the starting index of the files in cwa_root
#' @param f1 the ending index of the files in cwa_root
#'
#' @export
#'
run_stage_one<-function(cwa_root,results_root,json_args,f0,f1){
  ## handle missing f0/f1...
  if (missingArg(f0) || missingArg(f1)){
    stop('f0 and f1 need to be defined in run_stage_one')
  }

  # define the directores
  stage_root <- get_scratch_dir()
  stage_cwa <- file.path(stage_root,"accelerometer")
  stage_out <- file.path(stage_root,"out")


  if( getOption("test1_verbose",FALSE) ){
    message(" ====> staging data at: ",Sys.time())
    message("cwa_root: ",cwa_root)
    message("stage_root: ",stage_root)
    message("stage_cwa: ",stage_cwa)
    message("stage_out: ",stage_out)
  }
  if (!dir.exists(stage_cwa)){
    dir.create(stage_cwa)
  }
  if (!dir.exists(stage_out)) {
    dir.create(stage_out)
  }
  stage_files(fromDir = cwa_root, toDir = stage_cwa,f0=f0, f1=f1)
  if( getOption("test1_verbose",FALSE) ){
    message(" ====> Finished staging data at: ",Sys.time())
    print(dir(stage_cwa))
  }

  part1_args=list()
  if (!missingArg(json_args) && file.exists(json_args)){
    part1_args = jsonlite::fromJSON(json_args)
  }
  ## overwrite several options to avoid disaster...
  part1_args$mode=1
  part1_args$datadir=stage_cwa
  part1_args$outputdir=stage_out
  part1_args$f0=1
  part1_args$f1=length(dir(stage_cwa))

  if( getOption("test1_verbose",FALSE) ){
    print(unlist(part1_args))
    message(" ====> start part1 at: ",Sys.time())
  }
  do.call(GGIR::g.part1,part1_args)
  if( getOption("test1_verbose",FALSE) ){
    message(" ====> finished part1 at: ",Sys.time())
  }

  # stage the results...
  if (!dir.exists(results_root)){
    dir.create(results_root)
  }
  file.copy( dir(stage_out,full.names = T),results_root,recursive = T)
}
