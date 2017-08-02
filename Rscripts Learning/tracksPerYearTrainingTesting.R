#####################################################################
## MSD Tracks per Year Learning
#####################################################################

require(stringr, quietly = TRUE)
require(data.table, quietly = TRUE) 
require(plyr, quietly = TRUE)

require(tidyverse)
require(ggplot2)

# predefined variables and shared code
location = "laptop"
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
# Examine tracks per year dataset

tracks <- fread(file = "/home/bernhard/MSD/MSD/msd_tracks_per_year.csv")
(tracks_per_year <- table(tracks$year))
# tracks_per_year <- as_data_frame(tracks_per_year)
# colnames(tracks_per_year) = c("year", "number")

# plot(tracks_per_year, type = "l")

#####################################################################
# Try some basic machine learning - first bucketize 

breaks <- c(1900, 1960, 1970, 1980, 1990, 2000, 2005, 2020)
labels <- c("1990-1960", "1961-1970", "1971-1980", "1981-1990", 
            "1991-2000", "2001-2005", "2006-2011")
tracks$yearBucket <- cut(tracks$year, breaks = breaks, labels = labels, 
                      include.lowest = TRUE)
# table(tracks$yearBucket)
# hist(tracks$year, breaks = breaks)

###################################################################

features <- fread(featureFile)
# splits <- data.table(read.delim(splitFile, header = FALSE, 
#                                 comment.char = "%", 
#                                 stringsAsFactors = FALSE))
# colnames(splits) <- c("MSD_TRACKID", "SET")

#####################################################################
## Merge data and generate train and test datasets
#####################################################################

observations <- tracks %>% 
  left_join(features, by = "MSD_TRACKID") # add features
nrow(observations)

any(is.na(observations$yearBucket)) # check for observation where response is NA
sum(apply(observations, 1, function(x){any(is.na(x))})) # check for observations where features are NA

observations <- observations %>% drop_na()
nrow(observations)

train <- observations %>% sample_frac(0.9)
test <- anti_join(observations, train, by = "MSD_TRACKID")

# any(is.na(train$yearBucket))
table(train$yearBucket)
# any(is.na(test$yearBucket))
table(test$yearBucket)

rm(features); rm(observations) # remove unnecessary tables
#####################################################################
## Prepare machine learning information
#####################################################################

excludedCols <- c("MSD_TRACKID", "year", "artist", "song") # for the MSD datasets
response <- "yearBucket"
features <- setdiff(names(train), c(response, excludedCols))
mlformula <- reformulate(termlabels = features, response = response) # this is the formula 

#####################################################################
## Model training for Random Forest
#####################################################################

library(randomForest)

t1 <- Sys.time()
rfModel <- randomForest(mlformula, data = train, ntree = 100, 
                        importance = TRUE) 
rfDuration <- Sys.time() - t1
print(rfDuration)

#####################################################################
# Model testing for Random Forest
#####################################################################

# get information about model
summary(rfModel)
print(rfModel)
str(rfModel)

# cross check performance against train dataset
rfPredictTrain <- predict(rfModel, newdata = train, type="class")
round(sum(train[[response]] == rfPredictTrain)/length(rfPredictTrain) * 100, 2) # Overall recognition rate

# check error of model against test dataset
rfPredictTest <- predict(rfModel, newdata = test, type="class")
round(sum(test[[response]] == rfPredictTest)/length(rfPredictTest) * 100, 2) # Overall recognition rate

#####################################################################
