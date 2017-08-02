#####################################################################
## Load data of all testSeries into separate environments 
## and combine them to one data frame each
#####################################################################

rm(list = ls()) # clean complete global environment
includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

#####################################################################

applicationsEnv <- new.env() # these is the spark history information
jobsDetailsEnv <- new.env()
stagesDetailsEnv <- new.env()
jobsStagesDetailsEnv <- new.env()
outputParamsEnv <- new.env() # this is the output file information

platform <- "Scape01" # could also be "Local" or "Scape02" - change as needed

#####################################################################
## Functions
#####################################################################

loadCachedSeries <- function(data, platform,
                             directory, envir) {
  
  files <- list.files(path = directory,
                      # make sure to catch all testSeries but not the combined file if already there
                      pattern = paste0(data, "-", platform, "-"))
  # print(files)
  for (file in files) {
    load(file = file.path(directory, file),
         envir = envir)
  }
}

#####################################################################
## Load application information
#####################################################################

loadCachedSeries(data = "applications", platform = platform,
                 directory = cacheDir, envir = applicationsEnv)
ls(applicationsEnv)
object_size(applicationsEnv)

loadCachedSeries(data = "jobsDetails", platform = platform,
                 directory = cacheDir, envir = jobsDetailsEnv)
ls(jobsDetailsEnv)
object_size(jobsDetailsEnv)

loadCachedSeries(data = "stagesDetails", platform = platform,
                 directory = cacheDir, envir = stagesDetailsEnv)
ls(stagesDetailsEnv)
object_size(stagesDetailsEnv)

loadCachedSeries(data = "jobsStagesDetails", platform = platform,
                 directory = cacheDir, envir = jobsStagesDetailsEnv)
ls(jobsStagesDetailsEnv)
object_size(jobsStagesDetailsEnv)

loadCachedSeries(data = "outputParams", platform = platform,
                 directory = cacheDir, envir = outputParamsEnv)
ls(outputParamsEnv)
object_size(outputParamsEnv)

#####################################################################
## Combine all testSeries to one file
#####################################################################

applications <- bind_rows(as.list(applicationsEnv)) # this is so easy it is almost funny
nrow(applications)

jobsDetails <- bind_rows(as.list(jobsDetailsEnv))
nrow(jobsDetails)

stagesDetails <- bind_rows(as.list(stagesDetailsEnv))
nrow(stagesDetails)

jobsStagesDetails <- bind_rows(as.list(jobsStagesDetailsEnv))
nrow(jobsStagesDetails)

outputParams <- bind_rows(as.list(outputParamsEnv))
nrow(outputParams)

#####################################################################
## Save combined data for further processing
#####################################################################

savewithBackup(list = c("applications"),
               file = file.path(dataDir, paste0("applications-", platform, ".Rda")),
               envir = globalenv())
savewithBackup(list = c("jobsDetails"),
               file = file.path(dataDir, paste0("jobsDetails-", platform, ".Rda")),
               envir = globalenv())
savewithBackup(list = c("stagesDetails"),
               file = file.path(dataDir, paste0("stagesDetails-", platform, ".Rda")),
               envir = globalenv())
savewithBackup(list = c("jobsStagesDetails"),
               file = file.path(dataDir, paste0("jobsStagesDetails-",platform, ".Rda")),
               envir = globalenv())
savewithBackup(list = c("outputParams"),
               file = file.path(dataDir, paste0("outputParams-", platform, ".Rda")),
               envir = globalenv())

#####################################################################
## Additional functions
#####################################################################

extendOutParams <- function(data, separatedFields, lookupTableFile, allowedVars, dropCols) {
  # extend outParams with additional information 
  dataExt <- data %>%
    separate(appName, separatedFields, sep = "[ ]", remove = FALSE) %>%
    addNewData(lookupTableFile, allowedVars) %>%
    select(-one_of(dropCols)) %>%
    mutate(coresMax = str_replace(coresMax, "CoresMax", "")) %>% # get rid of unnecessary text
    mutate(execMem = str_replace(execMem, "ExecMem", ""))
  return(dataExt)
}

extendApplications <- function(appData, paramsData, separatedFields, dropCols) {
  dataExt <- appData %>%
    rename(appName = name) %>%
    unnest(attempts) %>%
    select(-one_of(dropCols)) %>%
    separate(appName, separatedFields, sep = "[ ]", remove = FALSE) %>% 
    mutate(coresMax = str_replace(coresMax, "CoresMax", "")) %>% # get rid of unnecessary text
    mutate(execMem = str_replace(execMem, "ExecMem", "")) %>%
    # join with outputParamsExt to further extend information
    inner_join(select(paramsData,-appName, -cluster),
              by = c("classification", "featuresName", "split", "algorithm", "coresMax", "execMem")) %>%
    distinct(id, .keep_all = TRUE) # if there are duplicates make sure that each application only appears once
  
  return(dataExt)
}

#####################################################################
## Extend output parameters
#####################################################################

outputParamsExtEnv <- new.env()

# various vectors of variable names to be used later
separatedFields <- c("cluster", "algorithm", "classification", 
                     "featuresName", "split", "coresMax", "execMem") 
lookupTableFile <- file.path(dataDir, "applicationLookupTable.csv")
allowedVars <- c("featuresName", "features", "classification", "split", "dimensions") # these are the columns to add to the dataframe
dropCols <- c("configFile", "featureFile", "labelFile", "splitFile")

for (testSeries in ls(outputParamsEnv)) {
  outputParamsExtEnv[[testSeries]] <- 
    extendOutParams(data = outputParamsEnv[[testSeries]], 
                    separatedFields = separatedFields,
                    lookupTableFile = lookupTableFile,
                    allowedVars = allowedVars,
                    dropCols = dropCols)
}

ls(outputParamsExtEnv)
object_size(outputParamsExtEnv)

#####################################################################
## Extend application information
#####################################################################

applicationsRuntimeExtEnv <- new.env()
dropCols <- c("sparkUser", "completed", "endTimeEpoch", "startTimeEpoch", "lastUpdatedEpoch")

for (testSeries in ls(applicationsEnv)) {
  if (!exists(testSeries, envir = outputParamsExtEnv)) {
    message(paste0("Missing Testseries: ", testSeries)); break()}
  applicationsRuntimeExtEnv[[testSeries]] <-
    extendApplications(appData = applicationsEnv[[testSeries]],
                       paramsData = outputParamsExtEnv[[testSeries]],
                       separatedFields = separatedFields,
                       dropCols = dropCols)
}

print("Check for incomplete Cases")
for (testSeries in ls(applicationsRuntimeExtEnv)) {
  print(paste0(testSeries, ":"))
  print(paste0("Initial Application Logs: ",
               nrow(applicationsEnv[[testSeries]]),
               " Initial Output Logs: ",
               nrow(outputParamsExtEnv[[testSeries]])))
  print(paste0("Final Cases: ", 
               nrow(applicationsRuntimeExtEnv[[testSeries]]),
               " Incomplete: ", 
               sum(!complete.cases(applicationsRuntimeExtEnv[[testSeries]]))))
}

ls(applicationsRuntimeExtEnv)
object_size(applicationsRuntimeExtEnv)

#####################################################################
## combine all testSeries and save
#####################################################################

applicationsRuntimeExt <- bind_rows(as.list(applicationsRuntimeExtEnv))
outputParamsExt <- bind_rows(as.list(outputParamsExtEnv))

savewithBackup(list = c("applicationsRuntimeExt"),
               file = file.path(dataDir, paste0("applicationsRuntimeExt-", platform, ".Rda")),
               envir = globalenv())
savewithBackup(list = c("outputParamsExt"),
               file = file.path(dataDir, paste0("outputParamsExt-", platform, ".Rda")),
               envir = globalenv())

#####################################################################
