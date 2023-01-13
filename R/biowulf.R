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
#' I use to worry about setting the scratch dir to a directory in /lscratch/JOB_ID/PROC_ID. I would
#' add the swarm array task id to the directory name.
#' since every instance of R has a unique tempdir, I dont need to work about this.
#' for simplicity just use the tempdir and scratch.  The new scratch directory is
#' /lscratch/JOB_ID/RtmpXXXX/scratch.  Each instance is unique.
#'
#' @export
#' @rdname scratch
#'
get_scratch_dir <- function(){
  fs::dir_create(tempdir(),"scratch")
}

