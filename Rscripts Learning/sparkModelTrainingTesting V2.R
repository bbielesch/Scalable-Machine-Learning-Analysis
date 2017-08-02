#####################################################################
## Spark Mllib Model Training and Testing
#####################################################################


# install.packages("DescTools")
# install.packages("tidyverse")
# install.packages("farff")
# install.packages("data.table")

# load helpful packages
require(DescTools)
require(data.table, quietly = TRUE)

# install necessary packages for Spark in R
# install.packages("sparklyr") # including all dependencies - but install from CRAN
# devtools::install_github("rstudio/sparklyr")
packageVersion("sparklyr")

#####################################################################

location <- "home" # can also be laptop or cluster, both are located in AIT
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))

#####################################################################
## Define access to spark and create connection
#####################################################################

# needs to be set for proper execution of sparklyr
if (location == "home") {
  Sys.setenv(SPARK_HOME = "/usr/local/spark")  
} else {
  Sys.setenv(SPARK_HOME = "/opt/spark-2.1")  
}

Sys.setenv(SPARK_MEM = "3g")
.libPaths(c(.libPaths()[1], file.path(Sys.getenv('SPARK_HOME'), 'R', 'lib'), .libPaths()[-1]))

require(sparklyr)
require(tidyverse) # includes dplyr and ggplot2 among others

# open spark context with custom config
config <- spark_config()
config$spark.executor.memory <- "6G" # allocate memory on each spark worker node
config$spark.driver.memory <- "3G" 
sc <- spark_connect(master = "local", config = config) # open spark context

#####################################################################
## Retrieve features, classification and split file
## and upload directly or indirectly to Spark
#####################################################################

partitions <- 30 # should be adapted according to the file size of the feature file

# copy features directly into spark due to memory size constraints
if (location == "cluster") {
  features.tbl <- spark_read_csv(sc, name = "features", 
                                 path = paste0(hdfsNameNode, featureFile),
                                 repartition = partitions)
} else { # everywhere else the csv file is also copied indirectly
  features <- data.table(read.csv(featureFile)) # would be the indirect way
  features.tbl <- copy_to(sc, features, overwrite = TRUE, repartition = partitions)
  rm(features) # cleanup
}

# as there is no read.delim for spark lets use an intermediate step
labels <- data.table(read.delim(labelFile, header = FALSE, comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(labels) <- c("MSD_TRACKID", "Genre")
labels.tbl <- copy_to(sc, labels, overwrite = TRUE, repartition = partitions)
rm(labels) # cleanup

splits <- data.table(read.delim(splitFile, header = FALSE, 
                                comment.char = "%", 
                                stringsAsFactors = FALSE))
colnames(splits) <- c("MSD_TRACKID", "SET")
splits.tbl <- copy_to(sc, splits, overwrite = TRUE, repartition = partitions)
rm(splits) # cleanup 

#####################################################################
## Merge data on spark and generate train and test datasets
#####################################################################

src_tbls(sc)
observations.tbl <- splits.tbl %>% 
  left_join(labels.tbl, by = "MSD_TRACKID") %>% # first add genre labels
  left_join(features.tbl, by = "MSD_TRACKID") # then add features

train.tbl <- observations.tbl %>% 
  filter(SET == "TRAIN") %>% # create subset based on tag
  select(-SET) # remove unnecessary columns

test.tbl <- observations.tbl %>% 
  filter(SET == "TEST") %>% # same as above
  select(-SET) # same as above

# db_drop_table(sc, "labels"); db_drop_table(sc, "features") # need to use the "real" names

####################################################################
## Machine Learning Preparations
####################################################################

response <- "Genre"
response_index <- paste0(response, "_idx")

features <- setdiff(colnames(train.tbl), 
                    c(response, "MSD_TRACKID", "SET")) 
# prevent model building on irrelevant columns

models <- list()

#####################################################################
## Random Forest in Spark
#####################################################################

# settings for random forest algorithm
num.trees = 500L # default 20
max.depth = 5L # default 5
max.bins = 32L # default 32

# Training model
t1 <- Sys.time()
models$rf_spark <- ml_random_forest(train.tbl, response = response, 
                                    features = features, num.trees = num.trees,
                                    max.depth = max.depth, max.bins = max.bins,
                                    type="classification")
rf_dur_model <- difftime(Sys.time(), t1, units = "secs")
message(paste0("Random Forest Model Building: ", rf_dur_model, " secs"))

# Inspect model
# models$rf_spark
# summary(models$rf_spark)
# str(models$rf_spark)

#####################################################################
## Decision Trees in Spark
#####################################################################

t1 <- Sys.time()
models$dt_spark <- ml_decision_tree(train.tbl, 
                                    response= response, 
                                    features = features,
                                    type = "classification")
dt_dur_model <- difftime(Sys.time(), t1, units = "secs")
message(paste0("Decision Tree Model Building: ", dt_dur_model, " secs"))

# Inspect model
# models$dt_spark
# summary(models$dt_spark)
# str(models$dt_spark)

#####################################################################
## Multi-layer perceptron in Spark
#####################################################################

# settings for multi-layer perceptron
layers <- c(length(features), 500, 500, 25)
# first figure corresponds to number of features
# last figure corresponds to the number of classes

t1 <- Sys.time()
models$mlp_spark <- ml_multilayer_perceptron(train.tbl,
                                             response= response, 
                                             features = features,
                                             layers = layers)
mlp_dur_model <- difftime(Sys.time(), t1, units = "secs")
message(paste0("Multilayer Perceptron Model Building: ", mlp_dur_model, " secs"))

# Inspect model
# models$mlp_spark
# summary(models$mlp_spark)
# str(models$mlp_spark)

dur_models <- c(rf_dur_model, dt_dur_model, mlp_dur_model) # retain all durations

#####################################################################
## End of model building
#####################################################################





#####################################################################
## Model Checking
#####################################################################

model_names <- c("rf_spark", "dt_spark", "mlp_spark")
for (i in 1:length(model_names)) {
  
  model <- models[[model_names[i]]] # pick next model using its name
  
  ## Check model against train dataset
  t1 <- Sys.time()
  predict_train <- sdf_predict(model, newdata = train.tbl) %>%
    collect
  dur_predict_train <- difftime(Sys.time(), t1, units = "secs")
  message(paste0("Model ",model_names[i], " Duration Prediction Train ", dur_predict_train, " secs"))
  
  # some magic with response and prediction
  predict_train[,response] <- factor(predict_train[[response]], 
                                     levels = model$model.parameters$labels)
  predict_train[,response_index] <- (as.integer(factor(predict_train[[response]])) - 1)
  
  # rf_pred_train_2 <- as.data.frame(table(rf_predict_train[[response_index]], rf_predict_train$prediction))
  # rf_pred_train_2$response <- response_index
  
  # remove all columns containing lists (as these cant be uploaded to spark)
  col_to_remove <- check_columns_to_remove(c("rawPrediction", "probability"), # these are the columns to remove
                                           colnames(predict_train)) # from all columns of data.table
  if (length(col_to_remove) > 0) {
    pred_train_sc <- select_(predict_train, .dots = col_to_remove) # the select_ function allows to supply the column names as variables  
  } else {
    pred_train_sc <- predict_train # no columns to remove
  }
  pred_train_sc <- copy_to(sc, pred_train_sc, overwrite = TRUE)
  
  # check important figures for prediction
  f1_train <- ml_classification_eval(pred_train_sc, response_index, "prediction", metric = "f1")
  wP_train <- ml_classification_eval(pred_train_sc, response_index, "prediction", metric = "weightedPrecision")
  wR_train <- ml_classification_eval(pred_train_sc, response_index, "prediction", metric = "weightedRecall")
  
  ## Check model against test dataset
  
  t1 <- Sys.time()
  predict_test <- sdf_predict(model, newdata = test.tbl) %>%
    collect
  dur_predict_test <- difftime(Sys.time(), t1, units = "secs")
  message(paste0("Model ",model_names[i], " Duration Prediction Test ", dur_predict_test, " secs"))
  
  # same magic with reponse and index
  predict_test[,response] <- factor(predict_test[[response]], 
                                    levels = model$model.parameters$labels)
  predict_test[,response_index] <- (as.integer(factor(predict_test[[response]])) - 1)
  
  # rf_pred_test_2 <- as.data.frame(table(rf_predict_train[[response_index]], rf_predict_train$prediction))
  # rf_pred_test_2$response <- response_index
  
  col_to_remove <- check_columns_to_remove(c("rawPrediction", "probability"), # these are the columns to remove
                                           colnames(predict_test)) # from all columns of data.table
  if (length(col_to_remove) > 0) {
    pred_test_sc <- select_(predict_test, .dots = col_to_remove) # the select_ function allows to supply the column names as variables  
  } else {
    pred_test_sc <- predict_test # no columns to remove
  }
  pred_test_sc <- copy_to(sc, pred_test_sc, overwrite = TRUE)
  
  f1_test <- ml_classification_eval(pred_test_sc, response_index, "prediction", metric = "f1")
  wP_test <- ml_classification_eval(pred_test_sc, response_index, "prediction", metric = "weightedPrecision")
  wR_test <- ml_classification_eval(pred_test_sc, response_index, "prediction", metric = "weightedRecall")
  
  #####################################################################
  ## and now put all results together into one table
  
  if (i == 1) { # first iteration - new data table
    durations <- data.table(dur_model = dur_models[i],
                            dur_predict_train = dur_predict_train, dur_predict_test = dur_predict_test,
                            f1_train = f1_train, wP_train = wP_train, wR_train = wR_train,
                            f1_test = f1_test, wP_test = wP_test, wR_test = wR_test)
    
  } else { # every other iteration - append to existing data table
    durations <- rbindlist(list(durations, 
                                list(dur_models[i], dur_predict_train, dur_predict_test,
                                     f1_train = f1_train, wP_train = wP_train, wR_train = wR_train,
                                     f1_test = f1_test, wP_test = wP_test, wR_test = wR_test)))
  }
}



#####################################################################
## Closing down connection
#####################################################################
spark_disconnect(sc)

