#####################################################################
##
## Extraction of information from Spark event logs - JSON
##
#####################################################################

rm(list = ls()) # clean complete global environment
includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

#####################################################################
## Utilize environments for processing history logs
#####################################################################

# only execute once at the beginning of the data collection activity
applicationsEnv <- new.env()
jobsDetailsEnv <- new.env()
stagesDetailsEnv <- new.env()
jobsStagesDetailsEnv <- new.env()

#####################################################################




#####################################################################

baseUrl <- "http://localhost:18080/api/v1/applications"

platform <- "Scape01" # could also be "Local" or "Scape02" - change as needed
testSeries <- "RF-Part2" # change accordingly - currently also "percentageSplit_0.9" and "fixedSplit_2000"

#####################################################################
## Functions to retrieve application information via REST API
#####################################################################

retrieveApplications <- function(baseUrl) {
  message("Retrieving applications...")
  con <- curl(baseUrl)
  appsRaw <- readLines(con)
  close(con)
  return(fromJSON(appsRaw))
}

#####################################################################
## functiont to retrieve job information for all supplied applications
#####################################################################

retrieveJobs <- function(applications, baseUrl) {
  message("Processing applications and retrieving jobs...")
  
  for (i in 1:nrow(applications)) {
    appProcessed <- applications[i, "id"]
    jobsUrl <- paste0(baseUrl, "/", appProcessed, "/jobs")
    message("(", i, ") Processing jobs for application: ", appProcessed)
    
    con <- curl(jobsUrl)
    jobsRaw <- readLines(con)
    close(con)
    
    if (i == 1) {
      jobsDetails <- cbind(appProcessed, fromJSON(jobsRaw))
    } else {
      jobsDetails <- rbind(jobsDetails, cbind(appProcessed, fromJSON(jobsRaw)))
    }
  }
  
  columnsConvert <- c("submissionTime", "completionTime") # just for documentation purposes
  jobsDetails %<>%
    rename(id = appProcessed) %>%
    mutate(id = as.character(id)) %>%
    mutate(submissionTime = ymd_hms(submissionTime, tz = "Europe/Vienna")) %>%
    mutate(completionTime = ymd_hms(completionTime, tz = "Europe/Vienna"))
  return(jobsDetails)
}

#####################################################################
## function to retrieve stages for all submitted applications
#####################################################################

retrieveStages <- function(applications, baseUrl) {
  message("Processing applications and retrieving stages...")
  
  for (i in 1:nrow(applications)) {
    appProcessed <- applications[i, "id"]
    stagesUrl <- paste0(baseUrl, "/", appProcessed, "/stages")
    message("(", i, ") Processing stages for application: ", appProcessed)
    
    con <- curl(stagesUrl)
    stagesRaw <- readLines(con)
    close(con)
    
    if (i == 1) {
      stagesDetails <- cbind(appProcessed, fromJSON(stagesRaw))
    } else {
      stagesDetails <- rbind(stagesDetails, cbind(appProcessed, fromJSON(stagesRaw)))
    }
  }
  
  columnsConvert <- c("submissionTime", "firstTaskLaunchedTime", "completionTime") # just for documentation purposes
  stagesDetails %<>% # changes stagesDetails table directly
    rename(id = appProcessed) %>%
    mutate(id = as.character(id)) %>%
    mutate(submissionTime = ymd_hms(submissionTime, tz = "Europe/Vienna")) %>%
    mutate(firstTaskLaunchedTime = ymd_hms(firstTaskLaunchedTime, tz= "Europe/Vienna")) %>%
    mutate(completionTime = ymd_hms(completionTime, tz = "Europe/Vienna"))
  
  return(stagesDetails)
}

#####################################################################
## Retrieve information from running history server
#####################################################################

if (exists("applicationsEnv"))
  applicationsEnv[[testSeries]] <- retrieveApplications(baseUrl)

if (exists("jobsDetailsEnv")) 
  jobsDetailsEnv[[testSeries]] <- retrieveJobs(applicationsEnv[[testSeries]], baseUrl)

if (exists("stagesDetailsEnv")) 
  stagesDetailsEnv[[testSeries]] <- retrieveStages(applicationsEnv[[testSeries]], baseUrl)

#####################################################################
## add job id to stagesDetails table
#####################################################################

if (exists("jobsStagesDetailsEnv")) {
jobsStagesDetailsEnv[[testSeries]] <- jobsDetailsEnv[[testSeries]] %>% 
  select(id, jobId, stageIds) %>%
  unnest(stageIds) %>%
  rename(stageId = stageIds) %>%
  right_join(stagesDetailsEnv[[testSeries]], by = c("id", "stageId"))
}

#####################################################################

## check environment
ls(applicationsEnv); object_size(applicationsEnv)
ls(jobsDetailsEnv); object_size(jobsDetailsEnv)
ls(stagesDetailsEnv); object_size(stagesDetailsEnv)
ls(jobsStagesDetailsEnv); object_size(jobsStagesDetailsEnv)

#####################################################################
## Store raw data
#####################################################################

# current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # currently not used
# data.table with all application information
savewithBackup(list = c(testSeries), 
               file = file.path(cacheDir, paste0("applications-", platform, "-", testSeries, ".Rda")),
               envir = applicationsEnv)
# data.table with job for all applications
savewithBackup(list = c(testSeries), 
               file = file.path(cacheDir, paste0("jobsDetails-", platform, "-", testSeries, ".Rda")),
               envir = jobsDetailsEnv)
# data.table with all stage information
savewithBackup(list = c(testSeries), 
               file = file.path(cacheDir, paste0("stagesDetails-", platform, "-", testSeries,".Rda")),
               envir = stagesDetailsEnv)
# previous data.table extended with job information - complete relationship between applications, jobs and stages
savewithBackup(list = c(testSeries), 
               file = file.path(cacheDir, paste0("jobsStagesDetails-", platform, "-",  testSeries, ".Rda")),
               envir = jobsStagesDetailsEnv)

#####################################################################



