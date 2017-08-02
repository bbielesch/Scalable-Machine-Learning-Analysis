#####################################################################
##
## Processing of Information 
## - extracted from history server
## - extracted from output parameters
## - merging of information
##
#####################################################################

rm(list = ls()) # clean global environment
includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

platform <- "Local" # could also be "Scape01" or "Scape02"

#####################################################################
## Load current output params data from file
#####################################################################

outputParamsFile <- file.path(dataDir, paste0("outputParams-", platform, ".Rda"))
lookupTableFile <- file.path(dataDir, "applicationLookupTable.csv")

load(file = outputParamsFile) # use current version
outputLookupTable <- fread(file = lookupTableFile) # just to check...

#####################################################################
# Enrich information with additional parameters
#####################################################################

# various vectors of variable names to be used later
separatedFields <- c("cluster", "algorithm", "classification", "featuresName", "split") 
allowedVars <- c("featuresName", "features", "classification", "split", "dimensions") # these are the columns to add to the dataframe
dropCols <- c("configFile", "featureFile", "labelFile", "splitFile")

# extend outParams with additional information
outputParamsExt <- outputParams %>%
  separate(appName, separatedFields, sep = "[ ]", remove = FALSE) %>%
  # the necessary functions are defined in .Rprofile
  addNewData(lookupTableFile, allowedVars) %>%
  select(-one_of(dropCols))

# checks of processed outputParams
str(outputParamsExt)
head(outputParamsExt)

#####################################################################

current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # not necessary
savewithBackup(outputParamsExt, 
               file = file.path(dataDir, paste0("outputParamsExt-", platform, ".Rda")))

#####################################################################





#####################################################################
## summary statistics per application
#####################################################################

load(file = file.path(cacheDir, paste0("applications-", platform, ".Rda")))

# make sure that the field names do not collide
applications %<>% 
  rename(appName = name)

applicationsRuntime <-
  applications %>%
  unnest(attempts) %>%
  select(-c(sparkUser, completed, endTimeEpoch, startTimeEpoch, lastUpdatedEpoch))

str(applicationsRuntime)
head(applicationsRuntime)

#####################################################################
# Further improvement of information -
# use appName as identifiers for subsetting
# Join outputParam and applicationsRuntimeExt to get a maximum of information
#####################################################################

# this is the information contained in the appName on Minbar
separatedFields <- c("cluster", "algorithm", "classification", "featuresName", "split") # these two fields are not present in all applications from Minbar

applicationsRuntimeExt <- applicationsRuntime %>% 
  separate(appName, separatedFields, sep = "[ ]", remove = FALSE) %>% # important, otherwise input column is removed, which is not what I want
  left_join(select(outputParamsExt,-appName, -cluster), 
            by = c("classification", "featuresName", "split", "algorithm")) # join with outputParamsExt to further extend information

str(applicationsRuntimeExt)
head(applicationsRuntimeExt)

Desc(applicationsRuntimeExt)
nrow(applicationsRuntimeExt[!complete.cases(applicationsRuntimeExt),]) # show some NA cases if any

#####################################################################

current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # not needed at the moment
savewithBackup(applicationsRuntimeExt, #, save result of processing
               file = file.path(dataDir, paste0("applicationsRuntimeExt-", platform, ".Rda")))

#####################################################################






#####################################################################
## summary statistics per application and job
#####################################################################

load(file = file.path(cacheDir, paste0("jobsDetails-",  platform, ".Rda")))
load(file = file.path(cacheDir, paste0("stagesDetails-", platform, ".Rda"))) # is this file still necessary
load(file = file.path(cacheDir, paste0("jobsStagesDetails-", platform, ".Rda")))

jobsDetails %<>%
  rename(jobName = name)

jobsRuntime <-
  jobsDetails %>%
  mutate(duration = as.integer((completionTime - submissionTime)*1000)) %>% # executorRunTime is in ms
  left_join(applications[,c("id", "appName")], by = c("id")) %>% # add application name
  select(id, appName, everything()) %>%
  setorder(id, jobId)

str(jobsRuntime)
head(jobsRuntime)

#####################################################################

current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # not necessary
savewithBackup(jobsRuntime, file = 
                 file.path(dataDir, paste0("jobsRuntime-", platform, ".Rda")))

####################################################################
