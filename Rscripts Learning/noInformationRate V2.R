#####################################################################
## Determining no-information rate
#####################################################################

rm(list = ls()) # clean global environment

# load helpful packages
require(DescTools)
require(tidyverse)
require(stringr)
require(data.table)

# devtools::install_github("mlr-org/mlr")
require(caret)
require(mlr)

# predefined variables and shared code
location = "laptop"
scriptsDir <- "Rscripts Learning"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
## Reading necessary files
#####################################################################

labelFileName <- "msd-topMAGD-genreAssignment.cls"
labelFile <- file.path(dataDir, labelSubDir, labelFileName)
labelsMASD <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(labelsMASD) <- c("MSD_TRACKID", "Genre")

labelFileName <- "msd-MAGD-genreAssignment.cls"
labelFile <- file.path(dataDir, labelSubDir, labelFileName)
labelsMAGD <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                                    stringsAsFactors = FALSE))
colnames(labelsMAGD) <- c("MSD_TRACKID", "Genre")

labelFileName <- "msd-topMAGD-genreAssignment.cls"
labelFile <- file.path(dataDir, labelSubDir, labelFileName)
labelsTopMAGD <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                                    stringsAsFactors = FALSE))
colnames(labelsTopMAGD) <- c("MSD_TRACKID", "Genre")

#####################################################################
## Check Distribution of Genres
#####################################################################

table(labelsMASD[["Genre"]])
noInfoMASD <- max(table(labelsMASD[["Genre"]]))/nrow(labelsMASD)
noInfoMASD

table(labelsMAGD[["Genre"]])
noInfoMAGD <- max(table(labelsMAGD[["Genre"]]))/nrow(labelsMAGD)
noInfoMAGD

table(labelsTopMAGD[["Genre"]])
noInfoTopMAGD <- max(table(labelsTopMAGD[["Genre"]]))/nrow(labelsTopMAGD)
noInfoTopMAGD