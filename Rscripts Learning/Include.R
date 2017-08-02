#####################################################################
## Common Information for several scripts
#####################################################################

# general directory information

# setting location dependent dir path for subdirectories for different files
if (location == "cluster") {
  hdfsNameNode <- "hdfs://s1node0:8020"
  hdfsDir <- "/user/bernhard" # copied all feature files to hdfs (/home/bernhard/features)
  dataDir <- "/home/bernhard/MSD"
  featureSubDir <- "features" 
} else if (location == "laptop") {
  dataDir <- "/home/bernhard/MSD"
  featureSubDir <- "MSD-TU/features"  
} else {
  dataDir <- "/home/berni/MSD"
  featureSubDir <- "MSD-TU/features"  
}

# these settings are the same independent of location
labelSubDir <- "MSD-TU/labels"
splitSubDir <- "MSD-TU/splits"

# retrieve directory information for all directories
featureFiles <- list.files(file.path(dataDir, featureSubDir))
labelFiles <- list.files(file.path(dataDir, labelSubDir))
splitFiles <- list.files(file.path(dataDir, splitSubDir))

#####################################################################
## Retrieve one single feature, label and split file
#####################################################################

# file names
featureFileName <- "msd_jmir_mfcc_all.csv" # all file names need to contain extension
featureFileName <- "msd_jmir_lpc_all.csv"
featureFileName <- "msd_jmir_spectral_all.csv"
# featureFileName <- "msd-marsyas-timbral.csv"

labelFileName <- "msd-topMAGD-genreAssignment.cls"
splitFileName <- "msd-topMAGD-partition_fixedSizeSplit_2000-v1.0.cls"

# Complete file names incl. directory
if (location == "cluster") { # as we are using hdfs on the cluster
  featureFile <- file.path(hdfsDir, featureSubDir, featureFileName)
} else {
  featureFile <- file.path(dataDir, featureSubDir, featureFileName)
}

# these settings are location-independent
labelFile <- file.path(dataDir, labelSubDir, labelFileName)
splitFile <- file.path(dataDir, splitSubDir, splitFileName)

#####################################################################
## Custom functions
#####################################################################

check_columns_to_remove <- function(col_to_remove, col_all) {
  col_return <- c()
  
  for (z in 1:length(col_to_remove)) {
    if (col_to_remove[z] %in% col_all) {
      col_return <- c(col_return, paste0("-", col_to_remove[z]))
    }
  }
  
  # return final columns to remove
  return(col_return) # could be empty
}

#####################################################################
## Normalization functions
#####################################################################

# scales::rescale is more flexible
normalize <- function (x) {
  (x-min(x))/(max(x)-min(x)) 
}

#####################################################################
