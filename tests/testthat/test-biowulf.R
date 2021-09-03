test_that("scratch dir exists and I can write to it",{
  scratch_dir <- get_scratch_dir()
  expect_true(dir.exists(scratch_dir))

  ## create a directory that does not exist
  mydir <- file.path(tempfile("stage",scratch_dir))
  dir.create(mydir)
  1:10 %>% purrr::walk(~file.create(file.path(mydir,paste0("file-",.x,".txt"))))
  expect_length(dir(mydir),10)
  ## clean up
  unlink( mydir,recursive = T)
})
