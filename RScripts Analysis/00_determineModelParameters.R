#####################################################################
##
## Determine Model Building Parameters
##
#####################################################################

# some basic parameters
includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

sourceDir <- file.path(Sys.getenv("HOME"), "MSD") # should be portable to different pc
featureSubDir <- file.path("MSD-TU", "features")  
labelSubDir <- file.path("MSD-TU", "labels")
splitSubDir <- file.path("MSD-TU", "splits")

#####################################################################
## Detailed determination of training and testing observations
#####################################################################

params <- data.table(labelFile = character(),
                     splitFile = character(),
                     trainObservations = double(),
                     testObservations = double(),
                     TotalObservations = double(),
                     stringsAsFactors=FALSE)

# retrieve directory information for all directories
labelFiles <- list.files(file.path(sourceDir, labelSubDir))
splitFiles <- list.files(file.path(sourceDir, splitSubDir))
# character vectors only contain file names but no path information

#####################################################################
## Retrieve features, classification and split file
#####################################################################

for (labelFile in labelFiles) {
  
  labels <- data.table(read.delim(file.path(sourceDir, labelSubDir, labelFile),
                                  header = FALSE,
                                  comment.char = "%",
                                  stringsAsFactors = FALSE))
  colnames(labels) <- c("MSD_TRACKID", "Genre") # file does not contain column header
  
  # determine correct split files to apply to this labels file
  if (str_detect(labelFile, "topMAGD")) {
    splitFilesSubset <- splitFiles[str_detect(splitFiles, "-topMAGD-")]
  } else if (str_detect(labelFile, "MASD")) {
    splitFilesSubset <- splitFiles[str_detect(splitFiles, "-MASD-")]
  } else { # must be MAGD
    splitFilesSubset <- splitFiles[str_detect(splitFiles, "-MAGD-")] 
  }
  
  for (splitFile in splitFilesSubset) {
    
    message(paste0("Labels: ", labelFile, " Splits: ", splitFile))
    splits <- data.table(read.delim(file.path(sourceDir, splitSubDir, splitFile),
                                    header = FALSE,
                                    comment.char = "%",
                                    stringsAsFactors = FALSE))
    colnames(splits) <- c("MSD_TRACKID", "SET") # file also does not contain column header
    
    #####################################################################
    ## Merge data and generate train and test datasets
    #####################################################################
    
    observations <- splits %>%
      left_join(labels, by = "MSD_TRACKID") # first add genre labels
    
    train <- observations %>%
      filter(SET == "TRAIN") %>% # create subset based on tag
      select(-SET) # remove unnecessary columns
    
    test <- observations %>%
      filter(SET == "TEST") %>% # same as above
      select(-SET) # same as above
    
    #####################################################################
    ## Retain required information
    #####################################################################
    
    params <- rbindlist(list(params,
                             list(labelFile,
                                  splitFile,
                                  nrow(train),
                                  nrow(test),
                                  nrow(observations))
    ))
    
    rm(list = c("train", "test", "observations")) # clean up for next run
    rm(splits) 
  } # for (splitFile in splitFiles)
  
  rm(labels)
} # for (labelFile in labelFiles)


#####################################################################
## Store compiled information as Rda
#####################################################################

# current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
savewithBackup(params, file = 
                 file.path(dataDir, paste0("modelParams-", platform, ".Rda")),
               envir = globalenv())
