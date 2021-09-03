#' Working with local scratch space
#'
#' @description These function work with the local scratch disk on the
#' compute node of biowulf.  Ideally, when you use the sinteractive,
#' sbatch, or swarm command, you remembered to use the
#' `--gres=lscratch:XXX`
#' option which allocates XXX GB of local scratch disk.  Another things to
#' remember is to use
#'
#' ```
#'  export TMPDIR=/lscratch/$SLURM_JOB_ID
#' ```
#'
#' in your batch file or issue the commnand interactively.  If you do this,
#' then tempfile() and tempdir() with create files appropriately.
#'
#' @details
#' If you are using swarm, the swarm task id is affixed the to scratch directory to prevent
#' the different processes from sharing a common scratch space.  Since you cannot control
#' whether or not all the swarm tasks are run on the same node, even using /tmp or /lscratch/JOB_ID
#' cannot guarantee that all the processes will have the same local scratch space.
#'
#' @note
#' I removed the clear_scratch_space function.  You cannot delete the /tmp or /lscratch/JOB_ID files.
#' It may do more harm than good.  Clean the space yourself.  Maybe work in a directory under the
#' scratch directory.
#'
#' @export
#' @rdname scratch
#'
get_scratch_dir <- function(){
  if (nchar(Sys.getenv("SLURM_ARRAY_TASK_ID"))>0 ){
    # we are running as part of a swarm
    # the local scratch space is /lscratch/${SLURM_JOB_ID}...
    # Create a subdirectory /lscratch/${SLURM_JOB_ID}/${SLURM_ARRAY_TASK_ID}
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

