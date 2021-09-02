
#' Get the local scratch dir on biowulf
#'
#' @return a scratch dir
#' @export
#'
#' @examples
get_scratch_dir <- function(){
  if (nchar(Sys.getenv("SLURM_ARRAY_TASK_ID"))>0 ){
    # we are running as part of a swarm
    # the local scratch space is /lscratch/${SLURM_JOB_ID}...
    # Create a subdirectory /lscratch/${SLURM_JOB_ID}/${SLURM_ARRAY_TAKS_ID}
    # if we have two jobs running on the same node, we dont want them copying to the
    # same directory...
    # this directory may not exist, so I may have to create it
    tmp_dir = paste0("/lscratch/",Sys.getenv("SLURM_JOB_ID"),"/",Sys.getenv("SLURM_ARRAY_TASK_ID"))
    if (!dir.exists(tmp_dir)) dir.create(tmp_dir)
  } else if (nchar(Sys.getenv("SLURM_JOB_ID"))>0 ) {
    # we are not part of a swarm but on a biowulf node ...
    # This directory will exist as long has you used
    #   --gres=lscratch:XXX with sbatch/slurm/sinteractive
    #     XXX = number of megabytes allocated
    tmp_dir = paste0("/lscratch/",Sys.getenv("SLURM_JOB_ID"))
  } else{
    ## not sure where I am...
    tmp_dir = tempdir()
  }
  tmp_dir
}


#' clean the scratch dir
#'
#' deletes all files in the scratch dir
#'
#' @export
#'
#' @examples
clean_scratch_dir <- function(){
  sdir <- get_scratch_dir()
  unlink(sdir,recursive = TRUE)
  dir.create(sdir)
}
