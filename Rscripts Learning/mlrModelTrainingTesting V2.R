#####################################################################
## mlr Model Training and testing
#####################################################################

# load helpful packages
require(DescTools)
require(tidyverse)
require(stringr)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
suppressMessages(require(data.table, quietly = TRUE))

# devtools::install_github("mlr-org/mlr")
require(caret)
require(mlr)

# predefined variables and shared code
location = "home"
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
## Reading necessary files
#####################################################################

features <- fread(featureFile)
labels <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(labels) <- c("MSD_TRACKID", "Genre")
splits <- data.table(read.delim(splitFile, header = FALSE, 
                                comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(splits) <- c("MSD_TRACKID", "SET")

#####################################################################
## Merge data and generate train and test datasets
#####################################################################

observations <- splits %>% 
  left_join(labels, by = "MSD_TRACKID") %>% # first add genre labels
  left_join(features, by = "MSD_TRACKID") # then add features

train <- observations %>% 
  filter(SET == "TRAIN") %>% # create subset based on tag
  select(-SET) %>% # remove unnecessary columns
  data.table

test <- observations %>% 
  filter(SET == "TEST") %>% # same as above
  select(-SET) %>% # same as above
  data.table

#####################################################################
## Prepare machine learning information
#####################################################################

excludedCols <- c("MSD_TRACKID") # for the MSD datasets
response <- "Genre"
features <- setdiff(names(train), c(response, excludedCols))

#####################################################################
## Setup machine learning tasks and retrieve some basic information
#####################################################################

listLearners()[c("class","package")]

# Create a task
trainTask <- makeClassifTask(data = as.data.frame(train[, (excludedCols) := NULL]), target = response)
testTask <- makeClassifTask(data = as.data.frame(test[, (excludedCols) := NULL]), target = response)

# Show various details for task
trainTask
getTaskDescription(trainTask)
getTaskFeatureNames(trainTask)

# Feature importance
imfeat <- generateFilterValuesData(trainTask, method = c("information.gain","chi.squared"))
plotFilterValues(imfeat,n.show = 20)

#####################################################################
## Regular decision tree in mlr
#####################################################################

getParamSet("classif.rpart") # package rpart needs to be installed
dtLearner <- makeLearner("classif.rpart", predict.type = "response",
                         fix.factors.prediction = TRUE)
dtLearner

# Set 3 fold cross validation
set_cv <- makeResampleDesc("CV",iters = 3L)

# Search for hyperparameters
dtParamSet <- makeParamSet(
  makeIntegerParam("minsplit",lower = 10, upper = 50),
  makeIntegerParam("minbucket", lower = 5, upper = 50),
  makeNumericParam("cp", lower = 0.001, upper = 0.2)
)

# Do a grid search
gscontrol <- makeTuneControlGrid()

# hypertune the parameters
t1 <- Sys.time()
dtTune <- tuneParams(learner = dtLearner, resampling = set_cv, task = trainTask, 
                     par.set = dtParamSet, control = gscontrol, measures = acc)
dtTuneDuration <- Sys.time() - t1
print(dtTuneDuration)

dtTune$x # best parameters
dtTune$y # check cv accuracy

#using hyperparameters for modeling
dtLearner <- setHyperPars(dtLearner, par.vals = dtTune$x)

# Train model
t1 <- Sys.time()
dtModel <- train(dtLearner, trainTask)
dtTrainDuration <- Sys.time() - t1
print(dtTrainDuration)

getLearnerModel(dtModel)

# Test model and check accuracy
dtPredict <- predict(dtModel, testTask)
performance(dtPredict) # report the misclassification error

#####################################################################
## Random Forest in mlr
#####################################################################

getParamSet("classif.randomForest")
rfLearner <- makeLearner("classif.randomForest", predict.type = "response",
                         fix.factors.prediction = TRUE,
                         par.vals = list(ntree = 100, mtry = 3, importance = TRUE))
rfLearner
getHyperPars(rfLearner) # check set parameters

# Set tunable parameters
# Grid search to find hyperparameters
rfParamSet <- makeParamSet(
  makeIntegerParam("ntree",lower = 50, upper = 500),
  makeIntegerParam("mtry", lower = 3, upper = 10),
  makeIntegerParam("nodesize", lower = 10, upper = 50)
)

# let's do random search for 50 iterations
rancontrol <- makeTuneControlRandom(maxit = 50L)
# set 3 fold cross validation
set_cv <- makeResampleDesc("CV",iters = 3L)

# hypertuning
t1 <- Sys.time()
rfTune <- tuneParams(learner = rfLearner, resampling = set_cv, task = trainTask, 
                     par.set = rfParamSet, control = rancontrol, measures = acc, 
                     show.info = FALSE)
rfTuneDuration <- Sys.time() - t1
print(rfTuneDuration)

rfTune$x # best parameters
rfTune$y # check cross-validation accuracy

# using hyperparameters for modeling
rfLearner <- setHyperPars(rfLearner, par.vals = rfTune$x)

# Train a model
t1 <- Sys.time()
rfModel <- train(rfLearner, trainTask)
rfTrainDuration <- Sys.time() - t1
print(rfTrainDuration)

getLearnerModel(rfModel)

# Test model and check accuracy
rfPredict <- predict(rfModel, testTask)
performance(rfPredict)

#####################################################################
## Random Forest based on ctrees in mlr
#####################################################################

getParamSet("classif.cforest")
crfLearner <- makeLearner("classif.cforest", predict.type = "response",
                          fix.factors.prediction = TRUE,
                          par.vals = list(ntree = 100, mtry = 3, importance = TRUE))

# Train a model
t1 <- Sys.time()
crfModel <- train(crfLearner, trainTask)
crfTrainDuration <- Sys.time() - t1
print(crfTrainDuration)

getLearnerModel(crfModel)

# Test model and check accuracy
crfPredict <- predict(crfModel, testTask)
performance(crfPredict)

#####################################################################
## k-Nearest Neighbor in mlr
#####################################################################

getParamSet("classif.knn")
knnLearner <- makeLearner("classif.knn", predict.type = "response")

# Train a model
t1 <- Sys.time()
knnModel <- train(knnLearner, trainTask)
knnTrainDuration <- Sys.time() - t1
print(knnTrainDuration)

getLearnerModel(knnModel)

# Test model and check accuracy
knnPredict <- predict(knnModel, testTask)
performance(knnPredict)

#####################################################################
## Another k-Nearest Neighbor in mlr
#####################################################################

getParamSet("classif.kknn")
kknnLearner <- makeLearner("classif.kknn", predict.type = "response")

# Train a model
t1 <- Sys.time()
knnModel <- train(kknnLearner, trainTask)
kknnTrainDuration <- Sys.time() - t1
print(kknnTrainDuration)

getLearnerModel(kknnModel)

# Test model and check accuracy
knnPredict <- predict(kknnModel, testTask)
performance(kknnPredict)

#####################################################################
## Fast k-Nearest Neighbor  in mlr
#####################################################################

getParamSet("classif.fnn")
fnnLearner <- makeLearner("classif.fnn", predict.type = "response")

# Train a model
t1 <- Sys.time()
fnnModel <- train(fnnLearner, trainTask)
fnnTrainDuration <- Sys.time() - t1
print(fnnTrainDuration)

getLearnerModel(fnnModel)

# Test model and check accuracy
crfPredict <- predict(crfModel, testTask)
performance(crfPredict)

#####################################################################
## SVM Support Vector Machine in mlr
#####################################################################

getParamSet("classif.ksvm") #  requires installed kernlab package
svmLearner <- makeLearner("classif.ksvm", predict.type = "response",
                          fix.factors.prediction = TRUE)

# Set parameters
svmParamSet <- makeParamSet(
  makeDiscreteParam("C", values = 2^c(-8,-4,-2,0)), # Cost parameters
  makeDiscreteParam("sigma", values = 2^c(-8,-4,0,4)) # RBF Kernel Parameter
)

# Specify search function
ctrl <- makeTuneControlGrid()

# Tune model
t1 <- Sys.time()
svmTune <- tuneParams(svmLearner, task = trainTask, resampling = set_cv, 
                      par.set = svmParamSet, control = ctrl, measures = acc)
svmTuneDuration <- Sys.time() - t1
print(svmTuneDuration)

svmTune$x # best parameters
smvTune$y # check cross-validation accuracy

# Set the model with best params
svmLearner <- setHyperPars(svmLearner, par.vals = svmTune$x)

# Train model
t1 <- Sys.time()
svmModel <- train(svmLearner, trainTask)
svmTrainDuration <- Sys.time() - t1
print(svmTrainDuration)

getLearnerModel(svmModel)

# Test model and check accuracy
svmPredict <- predict(svmModel, testTask)
performance(svmPredict)

#####################################################################
## GBM Gradient Boosted Machine in mlr
#####################################################################

getParamSet("classif.gbm")
gbmLearner <- makeLearner("classif.gbm", predict.type = "response",
                          fix.factors.prediction = TRUE)
gbmLearner

# Specify tuning method
rancontrol <- makeTuneControlRandom(maxit = 50L)

# 3-fold cross validation
set_cv <- makeResampleDesc("CV",iters = 3L)

# Parameters
gbmParamSet <- makeParamSet(
  makeDiscreteParam("distribution", values = "bernoulli"),
  makeIntegerParam("n.trees", lower = 100, upper = 1000), #number of trees
  makeIntegerParam("interaction.depth", lower = 2, upper = 10), #depth of tree
  makeIntegerParam("n.minobsinnode", lower = 10, upper = 80),
  makeNumericParam("shrinkage",lower = 0.01, upper = 1)
)

# Tune parameters
t1 <- Sys.time()
gbmTune <- tuneParams(learner = gbmLearner, task = trainTask, resampling = set_cv,
                      measures = acc, par.set = gbmParamSet, control = rancontrol)
gbmTuneDuration <- Sys.time() - t1
print(gbmTuneDuration)

gbmTune$x # Best parameters
gbmTune$y # check cross-validation accuracy

# Set parameters
gbmLearner <- setHyperPars(learner = gbmLearner, par.vals = gbmTune$x)

# Train model
t1 <- Sys.time()
gbmModel <- train(gbmLearner, trainTask)
gbmTrainDuration <- Sys.time() - t1
print(gbmTrainDuration)

getLearnerModel(gbmModel)

# Test model and check accuracy
gbmPredict <- predict(gbmModel, testTask)
performance(gbmPredict)

#####################################################################
## Xgboost
#####################################################################

set.seed(1001)
getParamSet("classif.xgboost")
xgLearner <- makeLearner("classif.xgboost", predict.type = "response",
                         fix.factors.prediction = TRUE)
xgLearner

xg_set$par.vals <- list(
  objective = "binary:logistic",
  eval_metric = "error",
  nrounds = 250
)

# Define parameters for tuning
xgParamSet <- makeParamSet(
  makeIntegerParam("nrounds",lower=200,upper=600),
  makeIntegerParam("max_depth",lower=3,upper=20),
  makeNumericParam("lambda",lower=0.55,upper=0.60),
  makeNumericParam("eta", lower = 0.001, upper = 0.5),
  makeNumericParam("subsample", lower = 0.10, upper = 0.80),
  makeNumericParam("min_child_weight",lower=1,upper=5),
  makeNumericParam("colsample_bytree",lower = 0.2,upper = 0.8)
)

# Define search function
rancontrol <- makeTuneControlRandom(maxit = 100L) #do 100 iterations

# 3-fold cross validation
set_cv <- makeResampleDesc("CV",iters = 3L)

# Tune parameters
t1 <- Sys.time()
xgTune <- tuneParams(learner = xgLearner, task = trainTask, resampling = set_cv,
                     measures = acc, par.set = xgParamSet, control = rancontrol)
xgTuneDuration <- Sys.time() - t1
print(xgTuneDuration)

xgTune$x # Best parameters
xgTune$y # check cross-validation accuracy

# Set parameters
xgLearner <- setHyperPars(learner = xgLearner, par.vals = xgTune$x)

# Train model
t1 <- Sys.time()
xgModel <- train(xgLearner, trainTask)
xgTrainDuration <- Sys.time() - t1
print(xgTrainDuration)

getLearnerModel(xgModel)

# Test model and check accuracy
xgPredict <- predict(xgModel, testTask)
performance(xgPredict)

#####################################################################
## And now the most generic approach - use a list to store all variables
#####################################################################

prefix <- "rf"
learner <- "classif.randomForest"
mlList <- list() # initialize empty list

getParamSet(learner)
mlList[[paste0(prefix, "Learner")]] <- makeLearner(learner, predict.type = "response",
                                                   fix.factors.prediction = TRUE, # these are not generic
                                                   par.vals = list(ntree = 100, mtry = 3, importance = TRUE))
mlList[[paste0(prefix, "Learner")]] # show details of learner

# Train a model
t1 <- Sys.time()
mlList[[paste0(prefix, "Model")]] <- train(mlList[[paste0(prefix, "Learner")]], trainTask)
mlList[[paste0(prefix, "TrainDuration")]] <- Sys.time() - t1
print(mlList[[paste0(prefix, "TrainDuration")]]) # print duration of training

getLearnerModel(mlList[[paste0(prefix, "Model")]]) # show details of model

# Test model and check accuracy
mlList[[paste0(prefix, "Predict")]] <- predict(mlList[[paste0(prefix, "Model")]], testTask)
performance(mlList[[paste0(prefix, "Predict")]])

mlList
str(mlList) # details of list generated (pretty long)
