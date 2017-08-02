#####################################################################
## Logistics Regression Training and Testing
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
location = "home"
scriptsDir <- "Rscripts Learning"
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
  mutate(Genre = as.factor(Genre)) %>%
  data.table

test <- observations %>% 
  filter(SET == "TEST") %>% # same as above
  select(-SET) %>% # same as above
  mutate(Genre = as.factor(Genre)) %>%
  data.table

#####################################################################
## Prepare machine learning information
#####################################################################

excludedCols <- c("MSD_TRACKID") # for the MSD datasets
response <- "Genre"
features <- setdiff(names(train), c(response, excludedCols))
mlformula <- reformulate(termlabels = features, response = response) # this is the formula 

#####################################################################
## Logistics Regression Model using nnet
#####################################################################

require(nnet)
logRegModelnnet <- multinom(mlformula, data = train, model = TRUE)

(sumLogRegModel <- summary(logRegModelnnet))

## z-test
# (z <- sumLogRegModel$coefficients/sumLogRegModel$standard.errors)

## 2-tailed z test
# (p <- (1 - pnorm(abs(z), 0, 1)) * 2)

## extract the coefficients from the model and exponentiate
# exp(coef(logRegModelnnet))

## Tidying the model
# require(broom)
# tidyLogRegModel <- tidy(logRegModelnnet)
# glance(logRegModelnnet)

####################################################################
## Prediction to Logistics Regression
####################################################################

## check prediction error against train dataset
logRegPredictTrain <- predict(logRegModelnnet, newdata = train, type="class")
round(sum(train[["Genre"]] == logRegPredictTrain)/length(logRegPredictTrain) * 100, 2) # Overall recognition rate

## Confusion Matrix for train dataset
Conf(logRegModelnnet)

## check error of model against test dataset
logRegPredictTest <- predict(logRegModelnnet, newdata = test, type="class")
round(sum(test[["Genre"]] == logRegPredictTest)/length(logRegPredictTest) * 100, 2) # Overall recognition rate

## Confusion Matrix for test dataset
Conf(x = logRegPredictTest, ref = test[["Genre"]])

