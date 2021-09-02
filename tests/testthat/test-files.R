# create the stage from/to with some files...
stage_set_up <- function(){
  root <- tempdir()
  stage_from <- file.path(root,"test_stage_from")
  stage_to <- file.path(root,"test_stage_to")
  dir.create(stage_from)
  dir.create(stage_to)
  file.create(file.path(stage_from,paste0("file",1:20,".txt")))
  return( list(from=stage_from,to=stage_to) )
}


data_set_up <- function(){
  stage <- file.path(tempdir(),"test_stage")
  dir.create(stage)
  base_file <- file.path(stage,"mydata.csv")
  data = tibble::tibble(x=1:100,y=101:200,z=201:300,group=rep(letters[1:10],each=10)) %>%
    dplyr::group_by(group) %>% dplyr::group_map(~.x) %>%
    #purrr::walk(~print(.x))
    #purrr::walk(~print(mangle_filename(base_file)))
    purrr::walk(~readr::write_csv(.x,mangle_filename(base_file)))
  return( list(stage_dir = stage, base_file=base_file ))
}

test_that("demangling a mangled filename returns the original filename",{
  files <- c("/data/file1.txt","~shmoopee/large_file.csv")
  expect_equal(demangle_filename(mangle_filename(files)),files)
})

test_that("staging copies files...",{

  setup=stage_set_up()
  stage_from = setup$from
  stage_to = setup$to
  ## clean up...
  withr::defer(unlink(stage_from,recursive = TRUE))
  withr::defer(unlink(stage_to,recursive = TRUE))

  files <- dir(stage_from,full.names = FALSE)
  ## more of a test of the setup..
  expect_length(files,20)
  stage_files(stage_from,stage_to)
  staged_files <- dir(stage_to,full.names = FALSE)
  expect_length(staged_files,length(files))

})

test_that("staging copies a subset of files...",{

  setup=stage_set_up()
  stage_from = setup$from
  stage_to = setup$to
  ## clean up...
  withr::defer(unlink(stage_from,recursive = TRUE))
  withr::defer(unlink(stage_to,recursive = TRUE))

  original_files <- dir(stage_from,full.names = FALSE)[3:7]
  stage_files(stage_from,stage_to,f0=3,f1=7)
  staged_files <- dir(stage_to,full.names = FALSE)
  expect_equal(staged_files,original_files)


  ## clean up...
  unlink(stage_from,recursive = TRUE)
  unlink(stage_to,recursive = TRUE)
})

test_that("staging can mangle files...",{
  setup=stage_set_up()
  stage_from = setup$from
  stage_to = setup$to

  ## clean up...
  withr::defer(unlink(stage_from,recursive = TRUE))
  withr::defer(unlink(stage_to,recursive = TRUE))

  files <- dir(stage_from,full.names = FALSE)
  stage_files(stage_from,stage_to, mangle = TRUE)
  staged_files <- dir(stage_to,full.names = FALSE)
  expect_false(isTRUE(all.equal(staged_files, files)))
  expect_length(staged_files,length(files))

  demangled <- demangle_filename(staged_files)
  expect_equal(demangled,files)
})

test_that("mangled files can be concatenated...",{
  setup <- data_set_up()
  stage_dir <- setup$stage_dir
  withr::defer(unlink(stage_dir,recursive = TRUE))

  base_file <- setup$base_file
  expect_length(dir(stage_dir),10)
  concat_mangled_files(fileDir = stage_dir,unmangled = base_file)
  file <- dir(stage_dir,pattern = basename(base_file),full.names = TRUE)
  expect_length(file,1)
  dta <- readr::read_csv(file)
  expect_equal(nrow(dta),100)
})
