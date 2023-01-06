#' Run GGIR on the NIH biowulf.
#' @name run GGIR
#'
#' @description
#' On the NIH biowulf, reading files directly from the /data disk is EXTREMELY slow.  So
#' we stage the data, files [f0..f1)  on local disk and run all the local files.
#'
#' The goal is to make a pipeline that is swarmable.  Nothing is returned by
#' this function, however, files are written in the results root.
#'
#' The output of run_stage_one should would with the input of run_stage_2_5.
#'
#' @details
#' The **output** directory used in parts 2-5 is different from the results root
#' from part 1.  The output directory is inside the results root and has a directory
#' path of ***RESULTS_ROOT/output_&lt;studyname&gt;***
#'
#' @param part1_output_dir the **output** directory from running part 1 of the GGIR
#' @param cwa_root where the CWA files are stored.
#' @param results_root where to write the results
#' @param json_args a json file containing a list of arguments passed into ggir
#' @param f0 the starting index of the files in cwa_root
#' @param f1 the ending index of the files in cwa_root
#'
#'
#' @rdname runggir
#' @order 1
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
  part1_args$do.parallel=FALSE

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

#' Empty the biowR cache
#'
#' removes files from the tools::R_user_dir("biowR") directory
#'
#' @return nothing returned
#' @export
#' @seealso [tools::R_user_dir()]
#'
clean_user_dir <- function(){
  if (dir.exists(tools::R_user_dir("biowR"))){
    files_to_delete <- dir(tools::R_user_dir("biowR"),full.names = TRUE)
    print(files_to_delete)
    unlink(files_to_delete)
  }
}

#' Prepares the swarmfile/script file for Stage 1 of the GGIR analysis
#'
#' Pay close attention to the parameters passed in. Currently, the code assumes that
#' each job takes 90 minutes to run.  This is to ensure that enough time is allocate to
#' run the job.
#'
#' Because CRAN policy does not allow the scripts to be placed in the user's home directory without
#' permission, the default location of the scripts is in tools::R_user_dir("biowR"), which on biowulf
#' is ~/share/R/biowR.  I would suggest setting the scriptDir argument to something useful like "~".
#'
#' @param scriptDir The directory where the scripts are written.
#' @param cwa_root  The directory where the accelerometer files are stored.
#' @param results_root  The root for the results directory.
#' @param json_args An optional json file with parameters for GGIR
#' @param f0 The start index
#' @param f1 The end index.  The file with index f1 is NOT run.
#' @param ncore The number of jobs you would like to swarm at once.
#' @param ht If you want to use hyper threading, set ht to TRUE.
#'
#' @return invisibly returns the name of the swarmfile
#' @export
#'
write_stage1_swarmfile <- function(scriptDir,cwa_root,results_root,json_args="",f0,f1,ncore,ht=FALSE){
    rscript <- write_stage1_R_script(scriptDir)
    swarmfile <- file.path(scriptDir,paste0("ggir_",f0,"_",f1,"_",ncore,".swarm"))

    ### calculate the number of per processor...
  njob=f1-f0-1
  if(ht){
    message("using hyperthreading ... ")
    ncore=2*ncore
  }
  nj<-rep(0,ncore)
  nj[1:(njob%%ncore)]<-1
  nj<-nj+rep(njob %/% ncore)

  breaks <- c(0,cumsum(nj))+f0
  start_job= breaks[1:ncore]
  end_job=breaks[2:length(breaks)]

  l <- paste0("Rscript ",rscript," ",cwa_root," ",results_root," ",start_job," ",end_job)
  if (file.exists(json_args)){
    l <- paste0(l," -json ",json_args)
  }

  writeLines(l,swarmfile)

  ## assume it takes 90 mins/job
  est=lubridate::as.period(lubridate::minutes(90*max(nj)),unit = "days")
  time_estimate=sprintf('%02d-%02d:%02d:%02d', lubridate::day(est), lubridate::hour(est), lubridate::minute(est), lubridate::second(est))
  job_name=paste0("ggir_swarm_",f0,"_",f1,"_",ncore)
  message("created files:\n\t ",rscript,"\n\t",swarmfile)
  message("Issue the following command:")
  if(ht){
    message('swarm -f ',swarmfile,' -g 16 --merge-output --logdir=',scriptDir,' --module R  --job-name ',job_name,'  --time ',time_estimate,' --gres=lscratch:500')
  } else{
    message('swarm -f ',swarmfile,' -p 2 -g 16 --merge-output --logdir=',scriptDir,' --module R  --job-name ',job_name,'  --time ',time_estimate,' --gres=lscratch:500')
  }

  invisible(swarmfile)
}


#' Write the R script for running GGIR
#' @name write_R_script
#'
#' @description
#' This is called from write_stage1_swarmfile or write_stage2_5_swarmfile
#'
#' @rdname write_swarmfile
#' @param scriptDir the directory where the R script is written
#' @seealso [write_stage1_swarmfile()] [write_stage2_5_swarmfile()]
#' @return the name of the script file (invisibly)
#'
write_stage1_R_script <- function(scriptDir=tools::R_user_dir("biowR")){
  if (!dir.exists(scriptDir)){
    dir.create(scriptDir,recursive = TRUE)
  }
  rscript=file.path(scriptDir,'run_ggir_stage_1.R')
  if (!file.exists(rscript)){
    writeLines(
'
#!/usr/bin/env Rscript
library("argparse")
library("biowR")


main<-function(args){
  if (!is.null(args$json) && !file.exists(args$json)) stop("Can not json parameter read file: ",args$json)
  if ( is.null(args$json) ){
    with(args,run_stage_one(cwa_root,results_root,f0 = f0,f1=f1))
  } else{
    with(args,run_stage_one(cwa_root,results_root,json_args = json,f0 = f0,f1=f1))
  }
}

parser <- ArgumentParser()
parser$add_argument("cwa_root",help="directory containing the CWA files")
parser$add_argument("results_root",help="location of GGIR results")
parser$add_argument("-json",help="path to json file containing parameters")
parser$add_argument("f0",type="integer",help="index of first file")
parser$add_argument("f1",type="integer",help="index of last file")

args<-parser$parse_args()
main(args)
',rscript)
  }

invisible(rscript)
}



#' @rdname write_swarmfile
#'
write_stage2_5_R_script <- function(scriptDir=tools::R_user_dir("biowR")){
  if (!dir.exists(scriptDir)){
    dir.create(scriptDir,recursive = TRUE)
  }
  rscript=file.path(scriptDir,'run_ggir_stage_1.R')
  if (!file.exists(rscript)){
    writeLines(
      '
#!/usr/bin/env Rscript
library("argparse")
library("biowR")


main<-function(args){
  if (!is.null(args$json) && !file.exists(args$json)) stop("Can not json parameter read file: ",args$json)
  if ( is.null(args$json) ){
    with(args,run_stage_one(cwa_root,results_root,f0 = f0,f1=f1))
  } else{
    with(args,run_stage_one(cwa_root,results_root,json_args = json,f0 = f0,f1=f1))
  }
}

parser <- ArgumentParser()
parser$add_argument("cwa_root",help="directory containing the CWA files")
parser$add_argument("results_root",help="location of GGIR results")
parser$add_argument("-json",help="path to json file containing parameters")
parser$add_argument("f0",type="integer",help="index of first file")
parser$add_argument("f1",type="integer",help="index of last file")

args<-parser$parse_args()
main(args)
',rscript)
  }

invisible(rscript)
}
