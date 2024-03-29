% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/ggir_functions.R
\name{stage_part1_results}
\alias{stage_part1_results}
\title{Stage results from GGIR part 1}
\usage{
stage_part1_results(output_dir, f0, f1)
}
\arguments{
\item{output_dir}{The output directory of the results}

\item{f0}{The index of the file in the directory (starting with 1)}

\item{f1}{The index of the last file in the directory}
}
\value{
invisibly returns the stage output directory
}
\description{
The way we run GGIR steps 2-5 on biowulf is to copy a set of results
from step 1 to a local staging scratch directory on one of the nodes.
The scratch directory is only available on the biowulf compute node.
Then steps 2-5 are run.

This function takes the results from step 1 and copies them on a local
scratch disk on a biowulf compute node.
}
\note{
The output directory is \strong{output_studyname}
}
