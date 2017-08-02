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

platform <- "Scape01" # could also be "Local" or "Scape02"

# The first part of the processing needs to be done in
# rawDataMerging-Scape01. That script needs to run before the current one

#####################################################################
## summary statistics per application and job
#####################################################################

# lead all necessary files for further processing
load(file = file.path(dataDir, paste0("applications-",  platform, ".Rda")))
load(file = file.path(dataDir, paste0("jobsDetails-",  platform, ".Rda")))
load(file = file.path(dataDir, paste0("stagesDetails-", platform, ".Rda"))) # is this file still necessary
load(file = file.path(dataDir, paste0("jobsStagesDetails-", platform, ".Rda")))

applications %<>%
  rename(appName = name)

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

# current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # not necessary
savewithBackup(jobsRuntime, 
     file = file.path(dataDir, paste0("jobsRuntime-", platform, ".Rda")))

####################################################################'