#####################################################################
## h2o Model Training and testing
#####################################################################

# load helpful packages
require(DescTools)
require(tidyverse)
require(stringr)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
suppressMessages(require(data.table, quietly = TRUE))

# predefined variables and shared code
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
## Reading necessary files
#####################################################################

features.dt <- fread(featureFile)
labels.dt <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                               stringsAsFactors = FALSE))
colnames(labels.dt) <- c("MSD_TRACKID", "Genre")
splits.dt <- data.table(read.delim(splitFile, header = FALSE, 
                                comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(splits.dt) <- c("MSD_TRACKID", "SET")

#####################################################################
## Merge data on spark and generate train and test datasets
#####################################################################

observations <- splits.dt %>% 
  left_join(labels.dt, by = "MSD_TRACKID") %>% # first add genre labels
  left_join(features.dt, by = "MSD_TRACKID") # then add features

train <- observations %>% 
  filter(SET == "TRAIN") %>% # create subset based on tag
  select(-SET) # remove unnecessary columns

test <- observations %>% 
  filter(SET == "TEST") %>% # same as above
  select(-SET) # same as above

#####################################################################
## Preparing machine learning
#####################################################################

# determine columns for response and features
excludedCols <- c("MSD_TRACKID") # for the MSD datasets
response <- "Genre"
features <- setdiff(colnames(train), c(response, excludedCols)) # make sure to only use the correct features

#####################################################################

# Load the H2O library and start up the H2O cluster locally on your machine
library(h2o)
h2o.init(nthreads = -1, #Number of threads -1 means use all cores on your machine
         max_mem_size = "8G")  #max mem size is the maximum memory to allocate to H2O

# uploading data to h2o, it only accepts h2o objects
train.h2o <- as.h2o(train)
test.h2o <- as.h2o(test)

#####################################################################
## h2o Random Forest
#####################################################################

# train a basis random forest retaining all default values
t1 <- Sys.time()
rfModel1.h2o <- h2o.randomForest(x = features,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "rfModel1.h2o",
                                 seed = 1) # required for reproducibility
rfDuration1 <- Sys.time() - t1
print(rfDuration1)

# train another Random Forest with more trees
t1 <- Sys.time()
rfModel2.h2o <- h2o.randomForest(x = features,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "rfModel2.h2o",
                                 #validation_frame = valid,  #only used if stopping_rounds > 0
                                 ntrees = 100,
                                 seed = 1)
rfDuration2 <- Sys.time() - t1
print(rfDuration2)

# Comparing the performance of the two RFs
rfPerf1.h2o <- h2o.performance(model = rfModel1.h2o,
                               newdata = test.h2o)
rfPerf2.h2o <- h2o.performance(model = rfModel2.h2o,
                               newdata = test.h2o)

# Print model performance
rfPerf1.h2o
rfPerf2.h2o

# Retreive test set AUC
h2o.auc(rfPerf1.h2o) 
h2o.auc(rfPerf2.h2o) 

#####################################################################
## Gradient Boosting Machine GBM with h2o
#####################################################################

t1 <- Sys.time()
gbmModel1.h2o <- h2o.gbm(x = features,
                         y = response,
                         training_frame = train.h2o,
                         model_id = "gbmModel1.h2o",
                         seed = 1)
gbmDuration1 <- Sys.time() - t1
print(gbmDuration1)

# another one with increased number of trees
t1 <- Sys.time()
gbmModel2.h2o <- h2o.gbm(x = features,
                         y = response,
                         training_frame = train.h2o,
                         model_id = "gbmModel2.h2o",
                         # validation_frame = valid,  #only used if stopping_rounds > 0
                         ntrees = 500,
                         seed = 1)
gbmDuration2 <- Sys.time() - t1
print(gbmDuration2)

# third one with increased number of trees but early stopping
t1 <- Sys.time()
gbmModel3.h2o <- h2o.gbm(x = features,
                         y = response,
                         training_frame = train.h2o,
                         model_id = "gbmModel3.h2o",
                         # validation_frame = valid,  #only used if stopping_rounds > 0
                         ntrees = 500,
                         score_tree_interval = 5,      #used for early stopping
                         stopping_rounds = 3,          #used for early stopping
                         stopping_metric = "AUC",      #used for early stopping
                         stopping_tolerance = 0.0005,  #used for early stopping
                         seed = 1)
gbmDuration3 <- Sys.time() - t1
print(gbmDuration3)

# Let's compare the performance of the three GBM models
gbmPerf1.h2o <- h2o.performance(model = gbmModel1.h2o,
                                newdata = test.h2o)
gbmPerf2.h2o <- h2o.performance(model = gbmModel2.h2o,
                                newdata = test.h2o)
gbmPerf3.h2o <- h2o.performance(model = gbmModel3.h2o,
                                newdata = test.h2o)

# Print model performance
gbmPerf1.h2o
gbmPerf2.h2o
gbmPerf3.h2o

# Retreive test set AUC
h2o.auc(gbmPerf1.h2o)
h2o.auc(gbmPerf2.h2o) 
h2o.auc(gbmPerf3.h2o) 

# scoring history
h2o.scoreHistory(gbmModel2.h2o)
h2o.scoreHistory(gbmModel3.h2o)

# Look at scoring history for third GBM model
plot(gbmModel3.h2o, timestep = "number_of_trees", metric = "AUC")
plot(gbmModel3.h2o, timestep = "number_of_trees", metric = "logloss")

#####################################################################
## Deep Learning with h2o
#####################################################################

t1 <- Sys.time()
dlModel1.h2o <- h2o.deeplearning(x = features,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "dlModel1.h2o",
                                 seed = 1)
dlDuration1 <- Sys.time() - t1
print(dlDuration1)

# Train another DL with new architecture and more epochs.
t1 <- Sys.time()
dlModel2.h2o <- h2o.deeplearning(x = features,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "dlModel2.h2o",
                                 # validation_frame = valid,  #only used if stopping_rounds > 0
                                 epochs = 20,
                                 hidden= c(10,10),
                                 stopping_rounds = 0,  # disable early stopping
                                 seed = 1)
dlDuration2 <- Sys.time() - t1
print(dlDuration2)

# Train a DL with early stopping
t1 <- Sys.time()
dlModel3.h2o <- h2o.deeplearning(x = features,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "dlModel3.h2o",
                                 # validation_frame = valid,  #in DL, early stopping is on by default
                                 epochs = 20,
                                 hidden = c(10,10),
                                 score_interval = 1,           #used for early stopping
                                 stopping_rounds = 3,          #used for early stopping
                                 stopping_metric = "AUC",      #used for early stopping
                                 stopping_tolerance = 0.0005,  #used for early stopping
                                 seed = 1)
dlDuration3 <- Sys.time() - t1
print(dlDuration3)

# Let's compare the performance of the three DL models
dlPerf1.h2o <- h2o.performance(model = dlModel1.h2o,
                               newdata = test.h2o)
dlPerf2.h2o <- h2o.performance(model = dlModel2.h2o,
                               newdata = test.h2o)
dlPerf3.h2o <- h2o.performance(model = dlModel3.h2o,
                               newdata = test.h2o)

# Print model performance
dlPerf1.h2o
dlPerf2.h2o
dlPerf3.h2o

# Retreive test set AUC
h2o.auc(dlPerf1.h2o)
h2o.auc(dlPerf2.h2o)
h2o.auc(dlPerf3.h2o)

# Scoring history
h2o.scoreHistory(dlModel3.h2o)
# Look at scoring history for third DL model
plot(dlModel3.h2o, 
     timestep = "epochs", 
     metric = "AUC")

#####################################################################
## Naive Bayes with h2o
#####################################################################

# First we will train a basic NB model with default parameters. 
t1 <- Sys.time()
nbModel1.h2o <- h2o.naiveBayes(x = features,
                               y = response,
                               training_frame = train.h2o,
                               model_id = "nbModel1.h2o")
nbDuration1 <- Sys.time() - t1
print(nbDuration1)

# Train a NB model with Laplace Smoothing
t1 <- Sys.time()
nbModel2.h2o <- h2o.naiveBayes(x = features,
                               y = response,
                               training_frame = train.h2o,
                               model_id = "nbModel2.h2o",
                               laplace = 6)
dlDuration2 <- Sys.time() - t1
print(dlDuration2)

# Let's compare the performance of the two NB models
t1 <- Sys.time()
nbPerf1.h2o <- h2o.performance(model = nbModel1.h2o,
                               newdata = test.h2o)
nbPerf2.h2o <- h2o.performance(model = nbModel2.h2o,
                               newdata = test.h2o)

# Print model performance
nbPerf1.h2o
nbPerf2.h2o

# Retreive test set AUC
h2o.auc(nbPerf1.h2o)
h2o.auc(nbPerf2.h2o)

#####################################################################

h2o.shutdown() # enough fun for now
