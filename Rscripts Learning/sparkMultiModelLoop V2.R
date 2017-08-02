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

location <- "cluster" # can also be laptop or cluster, both are located in AIT
scriptsDir <- "RScripts"
source(file.path(scriptsDir, "Include.R"))
if (!exists("dataDir")) {
  stop("Include file not found...")
}

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
config$spark.executor.memory <- "10G" # allocate memory on each spark worker node
config$spark.driver.memory <- "6G" 

if (location != "cluster")  {
  sc <- spark_connect(master = "local", config = config) # open spark context
} else { # connect to cluster node
  sc <- spark_connect(master = "spark://s1node0ex:7077", config = config) # open spark context on local cluster
}

#####################################################################
## Retrieve features, classification and split file
## and upload directly or indirectly to Spark
#####################################################################

partitions <- 200 # should be adapted according to the file size of the feature file

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

####################################################################
## Machine Learning Preparations
####################################################################

response <- "Genre"
response_index <- paste0(response, "_idx")

excludedCols <- c("MSD_TRACKID")
features <- setdiff(colnames(train.tbl), 
                    c(response, excludedCols)) 
# prevent model building on irrelevant columns

#####################################################################
## Random Forest in Spark
#####################################################################

start <- 50 # this many features in the first run
step <- 50 # jump that many features

feature_sequence <- seq(start, length(features), by = step)
if (feature_sequence[length(feature_sequence)] != length(features)) {
  feature_sequence <- c(feature_sequence, length(features)) # add 
}
num_trees_sequence <- c(100L, 200L, 300L, 400L, 500L) # default 20
max_depth_sequence <- c(5L) # default 5
# max_depth_sequence <- c(5L, 10L) # default 5
max_bins_sequence <- c(32L) # default 32
first <- TRUE # signal for creation of duration data.table

for (num_features in feature_sequence) {
  # settings for random forest algorithm
  for (num_trees in  num_trees_sequence) { 
    for (max_depth in max_depth_sequence) {
      for (max_bins in max_bins_sequence) { 
        
        # Training model
        t1 <- Sys.time()
        model <- ml_random_forest(train.tbl, response = response, 
                                  features = features[1:num_features], 
                                  num.trees = num_trees,
                                  max.depth = max_depth, 
                                  max.bins = max_bins,
                                  type="classification")
        dur_model <- difftime(Sys.time(), t1, units = "secs")
        message(paste0("Random Forest Model Building with ", 
                       num_features, " features, ",
                       num_trees, " trees", 
                       " maximum depth of ", max_depth,
                       " and maximum number of bins of ", max_bins, "."))
        
        if (first) { # first loop
          durations <- data.table(num_features = num_features, num_trees = num_trees, 
                                  max_depth = max_depth, max_bins = max_bins, dur_model = dur_model)
          first = FALSE
        } else { # all others
          durations <- rbindlist(list(durations, list(num_features,  num_trees, max_depth, max_bins, dur_model)))
        }
        
        save(durations, file= "Batch Job Running.Rda") # intermediate save in case of spark crash
      }
    }
  }
}

# Inspect model
# models$rf_spark
# summary(models$rf_spark)
# str(models$rf_spark)

#####################################################################
## End of model building
#####################################################################




#####################################################################
## Closing down connection
#####################################################################
spark_disconnect(sc)

