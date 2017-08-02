#####################################################################
## Model Testing
#####################################################################

# load helpful packages
require(DescTools)
require(stringr)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
suppressMessages(require(data.table, quietly = TRUE))

# predefined variables and shared code
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

# Training & testing data
train <- fread(inputTrainFile)
train[,"Genre"] <- as.factor(train[["Genre"]])
test <- fread(inputTestFile)
test[,"Genre"] <- as.factor(test[["Genre"]])


#####################################################################
## Model Testing for Decision Tree
#####################################################################

## Packages needed for this script
require(rpart)         # Popular decision tree algorithm
require(rpart.plot)    # Enhanced tree plots
require(party)         # Alternative decision tree algorithm
require(partykit)      # Convert rpart object to Binary Tree

load(file = file.path(cacheDir, "rpartModel.Rdata")) 

# get information about model
summary(rpartModel)
print(rpartModel)
str(rpartModel)

# cross check performance against train dataset
rpartPredictTrain <- predict(rpartModel, newdata = train, type="class")
round(sum(train[["Genre"]] == rpartPredictTrain)/length(rpartPredictTrain) * 100, 2) # Overall recognition rate

# check error of model against test dataset
rpartPredictTest <- predict(rpartModel, newdata = test, type="class")
round(sum(test[["Genre"]] == rpartPredictTest)/length(rpartPredictTest) * 100, 2) # Overall recognition rate

#####################################################################
## Model testing for SVM
#####################################################################

library(e1071)

load(file = file.path(cacheDir, "svmModel.Rdata")) 

# get information about model
summary(svmModel)
print(svmModel)
str(svmModel)

# cross check performance against train dataset
svmPredictTrain <- predict(svmModel, newdata = train)
round(sum(train[["Genre"]] == svmPredictTrain)/length(svmPredictTrain) * 100, 2) # Overall recognition rate

# check error of model against test dataset
svmPredictTest <- predict(rfModel, newdata = test)
round(sum(test[["Genre"]] == svmPredictTest)/length(svmPredictTest) * 100, 2) # Overall recognition rate

#####################################################################
## Model testing for Naive Bayes
#####################################################################

library(e1071)

load(file = file.path(cacheDir, "nbModel.Rdata")) 

# get information about model
summary(nbModel)
print(nbModel)
str(nbModel)

# cross check performance against train dataset
nbPredictTrain <- predict(nbModel, newdata = train)
round(sum(train[["Genre"]] == nbPredictTrain)/length(nbPredictTrain) * 100, 2) # Overall recognition rate

# check error of model against test dataset
nbPredictTest <- predict(nbModel, newdata = test)
round(sum(test[["Genre"]] == nbPredictTest)/length(nbPredictTest) * 100, 2) # Overall recognition rate

#####################################################################
## Model testing for Random Forest
#####################################################################

library(randomForest)

load(file = file.path(cacheDir, "rfModel.Rdata"))

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


