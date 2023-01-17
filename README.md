# biowR

An R package to Run GGIR on the NIH biowulf. The package may or may not work on your cluster

## Installing biowR

In order to install an R package from Github, you need the devtools package.

If you don't have the devtools package, run the following command:

    install.packages("devtools")

Once devtools is installed, to install biowR run the command:

    devtools::install_github("episphere/biowR")

## Running GGIR on biowulf

I **do not** recommend running GGIR with your accelerometer files on networked "/data" disk. The network disks are far slower than the local scratch disk. The biowR package moves all the input files (accelerometer files for stage1 or stage1 results for stages 2 ↔ 5.

### Running part 1

    script_dir <- "<directory where biowR writes the script files, swarm file,logs>"
                  default=tools::R_user_dir("biowR", "cache") or ~/.cache/R/biowR
    cwa_root <- "<directory containing accelerometer files>"
    results_root <- "<directory where biowR writes results>"
    json_args <- GGIR argmuments in JSON format
    f0 <- index of the first accelerometer file analyzed (R is 1-based, so first file index is 1 not 0)
    f1 <- index of the last accelerometer file analyzed (This file **is** run.  Analyzed data is [f0,f1])
    n_core <- number of cores we are requesting
    ht <- should we use hyperthreading.  (allocated 2 cpus/core allowing you to run 2 job/core). 
          When tested with the uk biobank data, there was no performance 
          degradation when using ht, so setting this to true essentially allows
          twice the throughput for the same number of cores.
          default=false
          
    write_stage1_swarmfile(cwa_root=cwa_root,results_root=results_root,json_args=json_args, 
                           f0=1, f2=10,n_core=5,ht=TRUE)

note that I am using the default script_dir and must use xx=yy for all arguments.

### Running part 2
The arguments are the same when you run stages 2->5 with the only exception of the output dir.

       output_dir <- <results_root from stage 1>/output_<studyname> where <studyname> 
       is currently set to "accelerometer"
       write_stage2_5_swarmfile(output_dir=output_dir,json_args=json_args, 
                                f0=1, f2=10,n_core=5,ht=TRUE)
