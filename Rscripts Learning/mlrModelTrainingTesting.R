#####################################################################
## mlr Model Training and testing
#####################################################################

# load helpful packages
require(DescTools)
require(stringr)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
suppressMessages(require(data.table, quietly = TRUE))

# devtools::install_github("mlr-org/mlr")
require(caret)
require(mlr)

# predefined variables and shared code
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
# this whole section is only used for testing the code with a small dataset

loans <- read.csv(file.path(scriptsDir, "loan.csv"))
loans[,"bad_loan"] <- as.factor(loans[["bad_loan"]])
sum(!complete.cases(loans)) # this is the overall number of incomplete cases
(n = nrow(loans))

# impute incomplete cases
str(loans)
sum(!complete.cases(loans)) # this is the number of incomplete cases
loansImp <- impute(loans, classes = list(factor = imputeMode(), integer = imputeMean(),
                                         numeric = imputeMean()))
loans <- loansImp$data
sum(!complete.cases(loans)) # check after imputing

set.seed(3859)
trainIndex = createDataPartition(loans$bad_loan, p = 0.7)$Resample
train = loans[trainIndex,]
test = loans[-trainIndex,]

# resync factor levels just to make sure
for (col in colnames(loans)) {
  if (is.factor(loans[,col])) {
    levels(train[,col]) <- levels(loans[,col])
    levels(test[,col]) <- levels(loans[,col])
  }
}

# optionally create new variables
train$Income_by_loan <- train$Total_Income/train$LoanAmount # Income by loan
test$Income_by_loan <- test$Total_Income/test$LoanAmount

train$Loan_Amount_Term <- as.numeric(train$Loan_Amount_Term) # Change variable class
test$Loan_Amount_Term <- as.numeric(test$Loan_Amount_Term)

train$Loan_amount_by_term <- train$LoanAmount/train$Loan_Amount_Term # Loan amount by term
test$Loan_amount_by_term <- test$LoanAmount/test$Loan_Amount_Term

# check the dataset
summarize(train) # this is the standard functionality
summarize(test)

Desc(train) # and this is the heavy gun
Desc(test)

# define response and features as variables
response <- "bad_loan"
features <- setdiff(names(loans), c(response, "int_rate"))
mlformula <- reformulate(termlabels = features, reponse = response) # this is the formula to use

# Tasks need to be created with identifying the correct positive classification
trainTask <- makeClassifTask(data = train, target = response, positive = 1)
testTask <- makeClassifTask(data = test, target = response, positive = 1)

#####################################################################

# test drive with even smaller dataset but with multiple classes 
data(iris)
(n = nrow(iris))

trainIndex = sample(1:n, size = round(0.7*n), replace=FALSE)
train = iris[trainIndex,]
test = iris[-trainIndex,]

response <- "Species"
features <- setdiff(names(iris), c(response))

trainTask <- makeClassifTask(data = train, target = response)
testTask <- makeClassifTask(data = test, target = response)

#####################################################################

# Training & testing data
train <- fread(inputTrainFile)
train[,"Genre"] <- as.factor(train[["Genre"]])
train <- train[,-c("MSD_TRACKID")]

test <- fread(inputTestFile)
test[,"Genre"] <- as.factor(test[["Genre"]])
test <- test[,-c("MSD_TRACKID")]

#####################################################################
## Setup machine learning tasks and retrieve some basic information
#####################################################################

listLearners()[c("class","package")]

# Create a task
trainTask <- makeClassifTask(data = train, target = response)
testTask <- makeClassifTask(data = test, target = response)

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
## And now the most generic approach - use a list of store all variables
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
