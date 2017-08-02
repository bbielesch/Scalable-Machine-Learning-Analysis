#####################################################################
## h2o Model Training and testing
#####################################################################

# load helpful packages
require(DescTools)
require(stringr)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
suppressMessages(require(data.table, quietly = TRUE))

# predefined variables and shared code
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
# this whole section is only used for testing the code with a small dataset

# loans <- read.csv(file.path(scriptsDir, "loan.csv"))
# loans[,"bad_loan"] <- as.factor(loans[["bad_loan"]])
# n = nrow(loans)
# trainIndex = sample(1:n, size = round(0.7*n), replace=FALSE)
# train = loans[trainIndex ,]
# test = loans[-trainIndex ,]
# 
# response <- "bad_loan"
# predictors <- setdiff(names(loans), c(response, "int_rate"))

#####################################################################

# Load the H2O library and start up the H2O cluster locally on your machine
library(h2o)
h2o.init(nthreads = -1, #Number of threads -1 means use all cores on your machine
         max_mem_size = "8G")  #max mem size is the maximum memory to allocate to H2O

# Training & testing data
train <- fread(inputTrainFile)
test <- fread(inputTestFile)

# Identify response and predictor variables

train[,"Genre"] <- as.factor(train[["Genre"]])
train.h2o <- as.h2o(train) # h2o only accepts h2o objects

test[,"Genre"] <- as.factor(test[["Genre"]])
test.h2o <- as.h2o(test)

# determine columns for response and predictors
excludedCols <- c("MSD_TRACKID") # for the MSD datasets
response <- "Genre"
predictors <- setdiff(colnames(train.h2o), c(response, excludedCols)) # make sure to only use the correct predictors

#####################################################################
## h2o Random Forest
#####################################################################

# train a basis random forest retaining all default values
t1 <- Sys.time()
rfModel1.h2o <- h2o.randomForest(x = predictors,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "rfModel1.h2o",
                                 seed = 1) # required for reproducibility
rfDuration1 <- Sys.time() - t1
print(rfDuration1)

# train another Random Forest with more trees
t1 <- Sys.time()
rfModel2.h2o <- h2o.randomForest(x = predictors,
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
gbmModel1.h2o <- h2o.gbm(x = predictors,
                         y = response,
                         training_frame = train.h2o,
                         model_id = "gbmModel1.h2o",
                         seed = 1)
gbmDuration1 <- Sys.time() - t1
print(gbmDuration1)

# another one with increased number of trees
t1 <- Sys.time()
gbmModel2.h2o <- h2o.gbm(x = predictors,
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
gbmModel3.h2o <- h2o.gbm(x = predictors,
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
dlModel1.h2o <- h2o.deeplearning(x = predictors,
                                 y = response,
                                 training_frame = train.h2o,
                                 model_id = "dlModel1.h2o",
                                 seed = 1)
dlDuration1 <- Sys.time() - t1
print(dlDuration1)

# Train another DL with new architecture and more epochs.
t1 <- Sys.time()
dlModel2.h2o <- h2o.deeplearning(x = predictors,
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
dlModel3.h2o <- h2o.deeplearning(x = predictors,
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
nbModel1.h2o <- h2o.naiveBayes(x = predictors,
                               y = response,
                               training_frame = train.h2o,
                               model_id = "nbModel1.h2o")
nbDuration1 <- Sys.time() - t1
print(nbDuration1)

# Train a NB model with Laplace Smoothing
t1 <- Sys.time()
nbModel2.h2o <- h2o.naiveBayes(x = predictors,
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
