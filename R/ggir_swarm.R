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
  if (methods::missingArg(f0) || methods::missingArg(f1)){
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
  if (!methods::missingArg(json_args) && file.exists(json_args)){
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
#' removes files from the tools::R_user_dir("biowR","cache") directory
#'
#' @return nothing returned
#' @export
#' @seealso [tools::R_user_dir()]
#'
clean_user_dir <- function(){
  if (dir.exists(tools::R_user_dir("biowR","cache"))){
    files_to_delete <- dir(tools::R_user_dir("biowR","cache"),full.names = TRUE)
    print(files_to_delete)
    unlink(files_to_delete)
  }
}

#' calculates the number of files to run on a hyperthread
#' n_jobs (integer)
#' n_cpu (integer)
#' returns a list containing a vector of integers startIndex of jobs
#'  and a list of the endIndex of a job.  This is INCLUSIVE.
#'
#' @param f0 - first index run
#' @param f1 - last index run
#' @param n_core - number of cores requested
#' @param ht - true/false use hyperthreading?

getStartAndEndJobs <- function(f0,f1,n_core,ht){
  ## calculate the number of jobs each core will run...
  n_jobs = f1 - f0 + 1

  ## turn off hyper threading if you have more cores than jobs
  ht = dplyr::if_else(n_core>n_jobs,FALSE,ht)
  ## if you have less jobs than cores, only use njob cores...
  n_core = min(n_core,n_jobs)

  ## the number of hyperthreads you will be using...
  n_cpu = dplyr::if_else(ht,2*n_core,n_core)

  ## calculate the number of jobs run on each hyperthread
  numJobsOnCpu<-rep(0,n_cpu)
  numJobsOnCpu[1:n_cpu<=(n_jobs%%n_cpu)]<-1
  numJobsOnCpu<-numJobsOnCpu+rep(n_jobs %/% n_cpu)
  numJobsOnCpu <- numJobsOnCpu[numJobsOnCpu>0]

  ## calculate the start/end index for each hyperthread
  startingIndexOnCpu <- f0+cumsum(numJobsOnCpu)-numJobsOnCpu
  lastIndexOnCpu <- f0+cumsum(numJobsOnCpu)-1

  return(list(start=startingIndexOnCpu,last=lastIndexOnCpu,num_jobs=numJobsOnCpu))
}


#' Ensure that the Path passed in is an absolute path
#'
#' @param path_in a possibly relative path
#'
#' @return an absolute path
ensure_absolute_path <- function(path_in){
  fs::path_expand(dplyr::if_else(fs::is_absolute_path(path_in), fs::path(path_in),fs::path_wd(path_in)))
}

#' Prepares the swarmfile/script file for Stage 1 of the GGIR analysis
#'
#' Pay close attention to the parameters passed in. Currently, the code assumes that
#' each job takes 120 minutes to run.  This is to ensure that enough time is allocate to
#' run the job.
#'
#' Because CRAN policy does not allow the scripts to be placed in the user's home directory without
#' permission, the default location of the scripts is in tools::R_user_dir("biowR","cache"), which on biowulf
#' is ~/.cache/R/biowR.  I would suggest setting the script_dir argument to something useful.
#'
#' @param script_dir The directory where the scripts are written.
#' @param cwa_root  The directory where the accelerometer files are stored.
#' @param results_root  The root for the results directory.
#' @param json_args An optional json file with parameters for GGIR
#' @param f0 The start index
#' @param f1 The end index.  The file with index f1 is run.
#' @param n_core The number of jobs you would like to swarm at once.
#' @param ht If you want to use hyper threading, set ht to TRUE.
#' @param output_dir The output_&lt;study$gt; directory in the result_root
#' @param indices A vector of file indices to (re)analyze
#' @param files The files names to (re)analyze.
#' directory from stage 1
#'
#' @return invisibly returns the name of the swarmfile
#' @export
#' @rdname write_swarmfile
#'
write_stage1_swarmfile <- function(script_dir=tools::R_user_dir("biowR","cache"),cwa_root,results_root,
                                   json_args="",f0,f1,n_core,ht=FALSE){

  ## guarantee that the argument are absolute paths...
  script_dir <- ensure_absolute_path(script_dir)
  cwa_root <- ensure_absolute_path(cwa_root)
  results_root <- ensure_absolute_path(results_root)
  json_args <- ensure_absolute_path(json_args)

  rscript <- write_stage1_R_script(script_dir)
  swarmfile <- file.path(script_dir,paste0("ggir_p1_",f0,"_",f1,"_",n_core,".swarm"))

  indices <- getStartAndEndJobs(f0,f1,n_core,ht)

  cat(paste0("Rscript ",rscript," ",cwa_root," ",results_root," ",indices$start," ",indices$last, " -json ",json_args),
      sep="\n",
      file = swarmfile)

  ## assume it takes 120 mins/job
  est_time_per_job=120
  est=lubridate::as.period(lubridate::minutes(est_time_per_job*max(indices$num_jobs)),unit = "days")
  time_estimate=sprintf('%02d-%02d:%02d:%02d', lubridate::day(est), lubridate::hour(est),
                        lubridate::minute(est), lubridate::second(est))
  job_name=paste0("ggir_p1_swarm_",f0,"_",f1,"_",length(indices$num_jobs))
  message("created files:\n\t ",rscript,"\n\t",swarmfile)
  message("Issue the following command:")
  if(ht){
    message('swarm -f ',swarmfile,' -p 2 -g 16 --merge-output --logdir=',script_dir,' --module R  --job-name ',job_name,'  --time ',time_estimate,' --gres=lscratch:500')
  } else{
    message('swarm -f ',swarmfile,' -g 16 --merge-output --logdir=',script_dir,' --module R  --job-name ',job_name,'  --time ',time_estimate,' --gres=lscratch:500')
  }

  invisible(swarmfile)
}

#' @export
#' @rdname write_swarmfile
write_stage2_5_swarmfile <- function(script_dir=tools::R_user_dir("biowR","cache"),output_dir,json_args,f0,f1,n_core,ht=FALSE){

  ## guarantee that the argument are absolute paths...
  script_dir <- ensure_absolute_path(script_dir)
  output_dir <- ensure_absolute_path(output_dir)
  json_args <- ensure_absolute_path(json_args)

  # output_dir must exist and have a meta/basic directory...
  if (!fs::dir_exists(fs::path(output_dir,"meta","basic")) ) stop("the output_dir does not contain a meta/basic directory")
  if ( length(fs::dir_ls(fs::path(output_dir,"meta","basic"))) == 0 ) stop("No part 1 results in the output_dir/meta/basic directory")

  # write the r script that the swarm will run
  rscript <- write_stage2_5_R_script(script_dir)

  ## make sure the arguments are absolute paths...
  if (!fs::is_absolute_path(script_dir))     script_dir <- fs::path_home(script_dir)
  if (!fs::is_absolute_path(output_dir))     output_dir <- fs::path_home(output_dir)
  if (!fs::is_absolute_path(json_args))      json_args <- fs::path_wd(json_args)

  ## calculate the number of jobs, first/last index on each hyperthread
  indices <- getStartAndEndJobs(f0,f1,n_core,ht)

  ## write the swarm file (note: n_core may no longer be correct)..
  swarmfile <- file.path(script_dir,paste0("ggir_p25_",f0,"_",f1,"_",length(indices$start),".swarm"))
  cat(paste0("Rscript ",rscript," ",output_dir," ",json_args," ",indices$start," ",indices$last),
      sep="\n",
      file = swarmfile)


  ## estimate time to run
  ## assume it takes 120 mins/job
  est_time_per_job=120
  est <- lubridate::as.period(lubridate::minutes(est_time_per_job * max(indices$num_jobs)),
                              unit = "days")
  time_estimate <- sprintf("%02d-%02d:%02d:%02d", lubridate::day(est),
                           lubridate::hour(est), lubridate::minute(est), lubridate::second(est))
  job_name = paste0("ggir_swarm_", f0, "_", f1, "_", n_core)
  message("created files:\n\t", rscript, "\n\t", swarmfile)
  message("\nIssue the following command:")
  if (ht) {
    message("swarm -f ", swarmfile, " -p 2 -g 16 --merge-output --logdir=",
            script_dir, " --module R  --job-name ", job_name,
            "  --time ", time_estimate, " --gres=lscratch:500")
  }
  else {
    message("swarm -f ", swarmfile, " -g 16 --merge-output --logdir=",
            script_dir, " --module R  --job-name ", job_name,
            "  --time ", time_estimate, " --gres=lscratch:500")
  }

  invisible(swarmfile)
}

#' @export
#' @rdname write_swarmfile
rewrite_stage1_swarmfile <- function(script_dir=tools::R_user_dir("biowR","cache"),cwa_root,results_root,
                                     json_args="",indices,files,ht=FALSE){

  ## you have to give either indices or filenames
  if ( length(files[!is.na(files)]) + length(files[!is.na(indices)]) == 0 ) return()
  if ( !fs::file_exists(json_args)) stop("json_args does not exist")

  script_dir <- ensure_absolute_path(script_dir)
  results_root <- ensure_absolute_path(results_root)
  json_args <- ensure_absolute_path(json_args)
  rscript <- write_stage1_R_script(script_dir)
  swarmfile <- file.path(script_dir,paste0("ggir_p1_",format(Sys.time(),"%Y%m%d_%H%M%OS3"),".swarm"))

  ## get a list of files...
  all_cwa <- fs::dir_ls(path=cwa_root,glob = "*.cwa")

  indices = as.integer(indices)
  indices = indices[!is.na(indices) & indices<=length(all_cwa) & indices > 0]
  from_indices <- all_cwa[indices]

  from_files <- fs::path(cwa_root,files)
  from_files <- intersect(all_cwa,from_files)

  indices_to_run <- sort( match(union(from_files,from_indices),all_cwa ) )

  cat(paste0("Rscript ",rscript," ",cwa_root," ",results_root," ",indices_to_run," ",indices_to_run, " -json ",json_args),
      sep="\n",
      file = swarmfile)

  ## assume it takes 120 mins/job
  time_estimate="2:00:00"
  job_name=paste0("ggir_p1_reswarm_",format(Sys.time(),"%Y%m%d_%H%M%OS3"))
  message("created files:\n\t ",rscript,"\n\t",swarmfile)
  message("Issue the following command:")
  if(ht){
    message('swarm -f ',swarmfile,' -p 2 -g 16 --merge-output --logdir=',script_dir,' --module R  --job-name ',job_name,'  --time ',time_estimate,' --gres=lscratch:500')
  } else{
    message('swarm -f ',swarmfile,' -g 16 --merge-output --logdir=',script_dir,' --module R  --job-name ',job_name,'  --time ',time_estimate,' --gres=lscratch:500')
  }

  invisible(swarmfile)
}

#' @export
#' @rdname write_swarmfile
rewrite_stage2_5_swarmfile <- function(script_dir=tools::R_user_dir("biowR","cache"),output_dir,json_args,indices,ht=FALSE){

  ## make sure the arguments are absolute paths...
  if (!fs::is_absolute_path(script_dir))     script_dir <- fs::path_home(script_dir)
  if (!fs::is_absolute_path(output_dir))     output_dir <- fs::path_home(output_dir)
  if (!fs::is_absolute_path(json_args))      json_args <- fs::path_wd(json_args)

  # output_dir must exist and have a meta/basic directory...
  if (!fs::dir_exists(fs::path(output_dir,"meta","basic")) ) stop("the output_dir does not contain a meta/basic directory")
  if ( length(fs::dir_ls(fs::path(output_dir,"meta","basic"))) == 0 ) stop("No part 1 results in the output_dir/meta/basic directory")

  # write the r script that the swarm will run
  rscript <- write_stage2_5_R_script(script_dir)

  ## get a list of part 1 results..
  all_rdata <- fs::dir_ls(fs::path(output_dir,"meta","basic"),glob = "*.RData")

  indices = as.integer(indices)
  indices = indices[!is.na(indices) & indices<=length(all_rdata) & indices > 0]
  from_indices <- all_rdata[indices]


  job_name <- paste0( "reswarm_p25_",format(Sys.time(),"%Y%m%d_%H%M%OS3") )
    ## write the swarm file (note: n_core may no longer be correct)..
  swarmfile <- file.path(script_dir,paste0(job_name,".swarm"))
  cat(paste0("Rscript ",rscript," ",output_dir," ",json_args," ",indices," ",indices,sep="\n"), file = swarmfile)


  ## estimate time to run
  ## assume it takes 120 mins/job
  est_time_per_job=120
  est <- lubridate::as.period(lubridate::minutes(est_time_per_job * length(indices) ),
                              unit = "days")
  time_estimate <- sprintf("%02d-%02d:%02d:%02d", lubridate::day(est),
                           lubridate::hour(est), lubridate::minute(est), lubridate::second(est))

  message("created files:\n\t", rscript, "\n\t", swarmfile)
  message("\nIssue the following command:")
  if (ht) {
    message("swarm -f ", swarmfile, " -p 2 -g 16 --merge-output --logdir=",
            script_dir, " --module R  --job-name ", job_name,
            "  --time ", time_estimate, " --gres=lscratch:500")
  }
  else {
    message("swarm -f ", swarmfile, " -g 16 --merge-output --logdir=",
            script_dir, " --module R  --job-name ", job_name,
            "  --time ", time_estimate, " --gres=lscratch:500")
  }

  invisible(swarmfile)
}




#' Write the R script for running GGIR
#' @name write_R_script
#'
#' @description
#' write_stage1_R_script and write_stage2_5_R_script are called from
#' write_stage1_swarmfile or write_stage2_5_swarmfile respectively.
#'
#' @param script_dir the directory where the R script is written
#' @seealso [write_stage1_swarmfile]
#' @return the name of the script file (invisibly)
#'
write_stage1_R_script <- function(script_dir=tools::R_user_dir("biowR","cache")){
  if (!dir.exists(script_dir)){
    dir.create(script_dir,recursive = TRUE)
  }
  rscript=file.path(script_dir,'run_ggir_stage_1.R')
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



#' @rdname write_R_script
#' @export
#'
write_stage2_5_R_script <- function(script_dir=tools::R_user_dir("biowR","cache")){
  if (!dir.exists(script_dir)){
    dir.create(script_dir,recursive = TRUE)
  }
  rscript=file.path(script_dir,'run_ggir_stage_2_5.R')
  if (!file.exists(rscript)){
    writeLines(
      '
library(argparser)
library(biowR)

validate_arguments <- function(args){
  print(args)
  if (!args$force && !grepl("output_[[:alnum:]]+$",args$output_dir) ) stop("output_dir is not not output_<studyname>")
  if (!fs::file_exists(args$output_dir)) stop("output_dir does not exist",args$output_dir)

  if (!fs::file_exists(args$params_file)) stop("params_file does not exist")

  if (args$f0<1) stop("f0 must be >=1")
  if (args$f1<args$f0) stop("f1 must be >= f1")
}

ggir_parts2_5 <- function(args){
  with(args,run_stages_2_5(part1_output_dir = output_dir,json_args = params_file,f0 = f0, f1=f1 ))
}


parser <- arg_parser("Run GGIR parts 2 through 5")
parser <- add_argument(parser,"output_dir",
                       help="directory containing the results of Part 1, should be output_<studyname>")
parser <- add_argument(parser,"params_file",help="path to json file containing parameters")
parser <- add_argument(parser,"f0",help="index of starting file",type="integer")
parser <- add_argument(parser,"f1",help="index of last file (inclusive)",type="integer")
parser <- add_argument(parser,"--force",help = "trust that the output_dir is correct",flag = TRUE)

args <- parse_args(parser)
validate_arguments(args)
ggir_parts2_5(args)
',rscript)
  }

invisible(rscript)
}
