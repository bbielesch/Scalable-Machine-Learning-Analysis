#####################################################################
## Model training in R      
#####################################################################

# load helpful packages
require(DescTools)
require(tidyverse)
require(stringr)
require(farff, quietly = TRUE) # new library with fast arff reading and writing capabilities
suppressMessages(require(data.table, quietly = TRUE))

# predefined variables
location <- "non-spark"
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
## Merge data on spark and generate train and test datasets
#####################################################################

observations <- splits %>% 
  left_join(labels, by = "MSD_TRACKID") %>% # first add genre labels
  left_join(features, by = "MSD_TRACKID") # then add features

train <- observations %>% 
  filter(SET == "TRAIN") %>% # create subset based on tag
  select(-SET) # remove unnecessary columns

test <- observations %>% 
  filter(SET == "TEST") %>% # same as above
  select(-SET) # same as above

#####################################################################
## Prepare machine learning information
#####################################################################

excludedCols <- c("MSD_TRACKID") # for the MSD datasets
response <- "Genre"
features <- setdiff(names(train), c(response, excludedCols))
mlformula <- reformulate(termlabels = features, response = response) # this is the formula 

#####################################################################
## Model training for SVM
#####################################################################

library(e1071)

t1 <- Sys.time()
svmModel <- svm(mlformula, data = train)
svmDuration <- Sys.time() - t1
print(svmDuration)

save(svmModel, file = file.path(cacheDir, "svmModel.Rdata")) 

#####################################################################
## Model training for Naive Bayes
#####################################################################

library(e1071) # same as above

t1 <- Sys.time()
nbModel <- naiveBayes(mlformula, data = train)
nbDuration <- Sys.time() - t1
print(nbDuration)

save(nbModel, file = file.path(cacheDir, "nbModel.Rdata")) 

#####################################################################
## Model training for k-NN
#####################################################################

# still open





#####################################################################
## Model training for decision tree
#####################################################################

# packages for decision trees
library(rpart)         # Popular decision tree algorithm
library(rpart.plot)    # Enhanced tree plots
library(party)         # Alternative decision tree algorithm
library(partykit)      # Convert rpart object to Binary Tree
library(evtree)        # Necessary for evolutionary learning tree
library(C50)           # Necessary for C5.0 model
library(RWeka)         # Weka decision tree J48

## Generating the rpart model
set.seed(1208)

t1 <- Sys.time()
rpartModel <- rpart(mlformula, data = train) 
rpartDuration <- Sys.time() - t1
print(rpartDuration)

save(rpartModel, file = file.path(cacheDir, "rpartModel.Rdata")) 

# Generating the c50 model
set.seed(1932)

t1 <- Sys.time()
c50Model <- C5.0(mlformula, data = train)
c50Duration <- Sys.time() - t1
print(c50Duration)

save(c50Model , file = file.path(cacheDir, "c50Model.Rdata")) 

# Generating the tree models implemented in Weka
set.seed(0800)

t1 <- Sys.time()
j48Model <- J48(mlformula, data = train)
j48Duration <- Sys.time() - t1
print(j48Duration)

save(j48Model , file = file.path(cacheDir, "j48Model.Rdata"))

t1 <- Sys.time()
lmtModel.lmt <- LMT(mlformula, data = train) 
lmtDuration <- Sys.time() - t1
print(lmtDuration)

save(lmtModel , file = file.path(cacheDir, "lmtModel.Rdata"))

# Generating the evtree model
set.seed(1492)

t1 <- Sys.time()
evtreeModel <- evtree(mlformula, data = train)
evtreeDuration <- Sys.time() - t1
print(evtreeDuration)

save(evtreeModel, file = file.path(cacheDir, "evtreeModel.Rdata"))

# Generating the ctree model
set.seed(0812)

t1 <- Sys.time()
ctreeModel <- ctree(train.sample$income.range ~ ., data = train)
ctreeDuration <- Sys.time() - t1
print(ctreeDuration)

save(ctreeModel, file = file.path(cacheDir, "ctreeModel.Rdata")) 

#####################################################################
## Model training for Random Forest
#####################################################################

library(randomForest)

t1 <- Sys.time()
rfModel <- randomForest(mlformula, data = train, ntree = 100, importance = TRUE) 
rfDuration <- Sys.time() - t1
print(rfDuration)

save(rfModel, file = file.path(cacheDir, "rfModel.Rdata"))
