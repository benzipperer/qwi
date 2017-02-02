# qwi_convert.R
# downloads and converts QWI data to Stata .dta format

rm(list = ls())
library(tidyverse)
library(haven)
library(foreach)
library(iterators)
library(data.table)
library(doMC)

# QWI version
version <- "R2016Q4"

# directory structure
home <- file.path("/data/qwi")
rawdata <- file.path(home,"raw",version)
statadata <- file.path(home,"stata",version)
filesep <- .Platform$file.sep

# Set number of cores to use
registerDoMC(cores=10)

# states to process
allstates <- read_csv(paste0(code,filesep,"allstates.csv"))
allstates <- allstates %>% transmute(stateabb=tolower(stateabb))
# filter available states in this release from all states
availablestates <- filter(allstates,stateabb!="id",stateabb!="mi",stateabb!="wy")
# test the function below on small subset of states
# availablestates <- filter(allstates,stateabb=="ak" | stateabb=="az")

# ownership categories to process
ownlist <- c("oslp","op")

# define function to download, convert, compress
# should probably be using temp files in it but haven't done that yet
# see http://stackoverflow.com/questions/3053833/using-r-to-download-zipped-data-file-extract-and-import-data
myfunction <- function(x) {
  
  # preliminaries
  stateabb <- x$stateabb
  own <- x$own
  basefile <- paste0("qwi_",stateabb,"_sa_f_gc_n3_",own,"_u")
  rawfile <- paste0(basefile,".csv.gz")
  statafile <- paste0(basefile,".dta")
  finalfile <- paste0(basefile,".dta.zip")
  url <- paste0("https://lehd.ces.census.gov/pub/",stateabb,"/",version,"/DVD-sa_f/",rawfile)
  rawdest <- paste0(rawdata,filesep,rawfile)
  finaldest <- paste0(statadata,filesep,finalfile)
 
   # download data
  system(paste("echo","Downloading ",url))
  download.file(url,rawdest)
  
  # convert to stata; compress
  gunzipcommand <- paste("gunzip -ck",rawdest)
  system(paste("echo","Converting to",statafile))
  write_dta(fread(gunzipcommand),statafile)
  system(paste("echo","Compressing",statafile))
  system(paste("zip",finalfile,statafile))
  file.copy(finalfile,finaldest,overwrite=TRUE)
  
  # clean up
  file.remove(statafile,finalfile)
}

mygrid <- expand.grid(stateabb=availablestates$stateabb,own=ownlist)
mcoptions <- list(preschedule=FALSE)
foreach(i=iter(mygrid, by="row"), .options.multicore=mcoptions) %dopar% myfunction(i)



