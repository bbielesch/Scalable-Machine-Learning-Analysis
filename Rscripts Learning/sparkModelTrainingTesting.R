#####################################################################
## Spark Mllib Model Training and Testing
#####################################################################

# install.packages("DescTools")
# install.packages("tidyverse")
# install.packages("farff")
# install.packages("data.table")

# load helpful packages
require(DescTools)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
require(data.table, quietly = TRUE)
# require(tidyverse)

# install necessary packages for Spark in R
# install.packages("sparklyr") # including all dependencies - but install from CRAN
# devtools::install_github("rstudio/sparklyr")
packageVersion("sparklyr")

#####################################################################

# general directory information
dataDir <- "/home/bernhard/MSD"
hdfsNameNode <- "hdfs://s1node0:8020"
hdfsDir <- "/user/bernhard" # copied all feature files to hdfs (/home/bernhard/features)
caching <- FALSE # caching of feature file with classification

# subdirectories for different files
featureSubDir <- "features"
cacheDir <- "cache"
labelSubDir <- "MSD-TU/labels"
splitSubDir <- "MSD-TU/splits"

# file names
featureFileName <- "msd-mvd.csv" # needs to contain extension csv
labelFileName <- "msd-MASD-styleAssignment.cls"
splitFileName <- "msd-MASD-partition_stratifiedPercentageSplit_0.9-v1.0.cls"
# cacheFileName <- paste0("MASD-fixedSplit-2000-","MSD_JMIR_MFCC_All") # no extension

# Complete file names incl. directory
featureFile <-  file.path(hdfsDir, featureSubDir, featureFileName)
labelFile <- file.path(dataDir, labelSubDir, labelFileName)
splitFile <- file.path(dataDir, splitSubDir, splitFileName)
# cacheFile <- file.path(hdfsDir, cacheDir, cacheFileName)

#####################################################################
## Define access to spark and copy train and test data to Spark
#####################################################################

# needs to be set for proper execution of sparklyr
Sys.setenv(SPARK_HOME = "/opt/spark-2.1")
Sys.setenv(SPARK_MEM = "3g")
.libPaths(c(.libPaths()[1], file.path(Sys.getenv('SPARK_HOME'), 'R', 'lib'), .libPaths()[-1]))

require(sparklyr)
require(dplyr)
require(ggplot2)

# open spark context with custom config
config <- spark_config()
config$spark.executor.memory <- "12G" # allocate memory on each spark worker node
config$spark.driver.memory <- "3G" 
sc <- spark_connect(master = "spark://s1node0ex:7077", config = config) # open spark context

#####################################################################
## Retrieve features, classification and split file
## and upload directly or indirectly to Spark
#####################################################################

# copy features directly into spark due to memory size constraints
# features <- data.table(read.csv(featureFile)) # would be the indirect way
features.tbl <- spark_read_csv(sc, name = "features", 
                               path = paste0(hdfsNameNode, featureFile),
                               repartition = 7)

# as there is no read.delim for spark lets use an intermediate step
labels <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(labels) <- c("MSD_TRACKID", "Genre")
labels.tbl <- copy_to(sc, labels, overwrite = TRUE, repartition = 7)
rm(labels) # cleanup

splits <- data.table(read.delim(splitFile, header = FALSE, 
                                comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(splits) <- c("MSD_TRACKID", "SET")
splits.tbl <- copy_to(sc, splits, overwrite = TRUE, repartition = 7)
rm(splits) # cleanup 

#####################################################################
## Merge data on spark and generate train and test datasets
#####################################################################

src_tbls(sc)
observations.tbl <- splits.tbl %>% 
  left_join(labels.tbl, by = "MSD_TRACKID") %>% # first add genre labels
  left_join(features.tbl, by = "MSD_TRACKID") # then add features

train.tbl <- observations.tbl %>% filter(SET == "TRAIN")
test.tbl <- observations.tbl %>% filter(SET == "TEST")

if (caching) {
  spark_write_csv(train.tbl, paste0(hdfsNameNode, cacheFile, "-train.csv"))
  spark_write_csv(test.tbl, paste0(hdfsNameNode, cacheFile, "-test.csv"))
}

# db_drop_table(sc, "labels"); db_drop_table(sc, "features") # need to use the "real" names

#####################################################################
## Block necessary when reading data from prepared train and test file
#####################################################################

# scriptsDir <- "RScripts"
# source(file.path(scriptsDir, "Include.R")) # predefined variables and shared code
# 
# train <- fread(inputTrainFile)
# train[,"Genre"] <- as.factor(train[["Genre"]])
# test <- fread(inputTestFile)
# test[,"Genre"] <- as.factor(test[["Genre"]])
# 
# train.tbl <- copy_to(sc, train) # and copy both datasets to Spark
# test.tbl <- copy_to(sc, test)

####################################################################
## Machine Learning Preparations
####################################################################

response <- "Genre"
features <- setdiff(colnames(train.tbl), 
                    c(response, "MSD_TRACKID", "SET")) 
# prevent model building on irrelevant columns

#####################################################################
## Random Forest in Spark
#####################################################################

# settings for random forest algorithm
num.trees = 500L # default 20
max.depth = 5L # default 5
max.bins = 32L # default 32

# Training model
t1 <- Sys.time()
rfModel.spark <- ml_random_forest(train.tbl, response = response, 
                                  features = features, num.trees = num.trees,
                                  max.depth = max.depth, max.bins = max.bins,
                                  type="classification")
durModel <- Sys.time() - t1
print(durModel)

# Inspect model
rfModel.spark
summary(rfModel.spark)
str(rfModel.spark)

# Check model against train dataset
# t1 <- Sys.time()
# rfPredictTrain <- sdf_predict(rfModel.spark, newdata = train.tbl) %>%
#   collect
# durPredictTrain <- Sys.time() - t1
# print(durPredictTrain)

# prepare data for confusion table.....
# rfPredictTrain[,"Genre"] <- factor(rfPredictTrain[["Genre"]], 
#                                    levels = rfModel.spark$model.parameters$labels)
# rfPredictTrain[,"Genre_idx"] <- (as.integer(factor(rfPredictTrain[["Genre"]])) - 1)
# 
# table(rfPredictTrain$Genre_idx, rfPredictTrain$prediction)
# (sum(rfPredictTrain$Genre_idx == rfPredictTrain$prediction))/nrow(rfPredictTrain)

# Check model against test dataset
t1 <- Sys.time()
rfPredictTest <- sdf_predict(rfModel.spark, newdata = test.tbl) %>%
  collect
durPredictTest <- Sys.time() - t1
print(durPredictTest)

# prepare data for confusion table.....
rfPredictTest[,"Genre"] <- factor(rfPredictTest[["Genre"]], 
                                  levels = rfModel.spark$model.parameters$labels)
rfPredictTest[,"Genre_idx"] <- (as.integer(factor(rfPredictTest[["Genre"]])) - 1)

table(rfPredictTest$Genre_idx, rfPredictTest$prediction)
(accPredictTest <- (sum(rfPredictTest$Genre_idx == rfPredictTest$prediction))/nrow(rfPredictTest))

rfScores <- list(model = rfModel.spark,
                 dur.model = durModel, 
                 dur.pred.test = durPredictTest,
                 acc.pred.test = accPredictTest)

#####################################################################
## Decision Trees in Spark
#####################################################################

t1 <- Sys.time()
dtModel.spark <- ml_decision_tree(train.tbl, 
                                  response= response, 
                                  features = features)
durModel <- Sys.time() - t1
print(durModel)

# Inspect model
dtModel.spark
summary(dtModel.spark)
str(dtModel.spark)

# checking against train dataset
# t1 <- Sys.time()
# dtPredict <- sdf_predict(dtModel.spark, newdata = train.tbl) %>%
#   collect
# durPredictTrain <- Sys.time() - t1
# print(durPredictTrain)

# checking against test dataset
t1 <- Sys.time()
dtPredictTest <- sdf_predict(dtModel.spark, newdata = test.tbl) %>%
  collect
durPredictTest <- Sys.time() - t1
print(durPredictTest)

# prepare data for confusion table.....
dtPredictTest[,"Genre"] <- factor(dtPredictTest[["Genre"]], 
                                  levels = dtModel.spark$model.parameters$labels)
dtPredictTest[,"Genre_idx"] <- (as.integer(factor(dtPredictTest[["Genre"]])) - 1)

table(dtPredictTest$Genre_idx, dtPredictTest$prediction)
(accPredictTest <- (sum(dtPredictTest$Genre_idx == dtPredictTest$prediction))/nrow(dtPredictTest))

dtScores <- list(model = dtModel.spark,
                 dur.model = durModel, 
                 dur.pred.test = durPredictTest,
                 acc.pred.test = accPredictTest)

#####################################################################
## Multi-layer perceptron in Spark
#####################################################################

# settings for multi-layer perceptron
layers <- c(length(features), 500, 500, 25)
# first figure corresponds to number of features
# last figure corresponds to the number of classes

t1 <- Sys.time()
mlpModel.spark <- ml_multilayer_perceptron(train.tbl,
                                           response= response, 
                                           features = features,
                                           layers = layers)
durModel <- Sys.time() - t1
print(durModel)

# Inspect model
mlpModel.spark
summary(mlpModel.spark)
str(mlpModel.spark)

# checking against train dataset
# t1 <- Sys.time()
# mlpPredictTrain <- sdf_predict(mlpModel.spark, newdata = train.tbl) %>%
#   collect
# durPredictTrain <- Sys.time() - t1
# print(durPredictTrain)

# checking against test dataset
t1 <- Sys.time()
mlpPredictTest <- sdf_predict(mlpModel.spark, newdata = test.tbl) %>%
  collect
durPredictTest <- Sys.time() - t1
print(durPredictTest)

# prepare data for confusion table.....
mlpPredictTest[,"Genre_factor"] <- factor(mlpPredictTest[["Genre"]], 
                                   levels = mlpModel.spark$model.parameters$labels)
mlpPredictTest[,"Genre_idx"] <- (as.integer(factor(mlpPredictTest[["Genre_factor"]])) - 1)

table(mlpPredictTest$Genre_idx, mlpPredictTest$prediction)
(accPredictTest <- (sum(mlpPredictTest$Genre_idx == mlpPredictTest$prediction))/nrow(mlpPredictTest))

mlpScores <- list(model = mlpModel.spark,
                  dur.model = durModel, 
                  dur.pred.test = durPredictTest,
                  acc.pred.test = accPredictTest)

#####################################################################
## Final Scores (Duration and Accuracy)
#####################################################################

Scores <- list(rf = rfScores, dt = dtScores, mlp = mlpScores)

#####################################################################
## Closing down connection
#####################################################################
spark_disconnect(sc)



#####################################################################
## Surplus - Naive Bayes in Spark
#####################################################################

# Train model
t1 <- Sys.time()
nbModel.spark <- ml_naive_bayes(train.tbl, 
                                response= response, 
                                features = features)
durModel <- Sys.time() - t1
print(durModel)

# Inspect model
nbModel.spark
summary(nbModel.spark)
str(nbModel.spark)

# check model against train dataset
t1 <- Sys.time()
nbPredictTrain <- sdf_predict(nbModel.spark, newdata = train.tbl) %>%
  collect
durPredictTrain <- Sys.time() - t1
print(durModel)

# check model against test dataset
t1 <- Sys.time()
nbPredictTest <- sdf_predict(nbModel.spark, newdata = test.tbl) %>%
  collect
durPredictTest <- Sys.time() - t1
print(durModel)

# prepare data for confusion table.....
nbPredictTest[,"Genre"] <- factor(nbPredictTest[["Genre"]], 
                                  levels = nbModel.spark$model.parameters$labels)
nbPredictTest[,"Genre_idx"] <- (as.integer(factor(nbPredictTest[["Genre"]])) - 1)

table(nbPredictTest$Genre_idx, nbPredictTest$prediction)
(sum(nbPredictTest$Genre_idx == nbPredictTest$prediction))/nrow(nbPredictTest)

#####################################################################
