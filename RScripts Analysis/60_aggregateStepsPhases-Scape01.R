#####################################################################
##
## Processing of Information 
## - aggregate jobs information into steps of machine learning pipeline
##
#####################################################################

rm(list = ls()) # clean global environment
includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

platform <- "Scape01" # could also be "Scape01" or "Scape02"

#####################################################################
## Functions
#####################################################################

addColumn <- function(data, columnName, fromStart, fromEnd, entries) {
  if ((length(fromStart) + length(fromEnd) + 1) != length(entries)) break() # parameters don't match
  
  data[[columnName]] <- rep(NA, nrow(data)) # add empty column
  data <- data[order(data[["jobId"]]),] # and order by jobId to establish correct sequence
  
  # first deal with replacements from the beginning
  index <- 1
  for (increment in fromStart) {
    data[index:(index + increment - 1), columnName] <- entries[1]
    entries <- entries[-1]
    index <- index + increment
  }
  
  # then with replacements from the end
  index <- nrow(data)
  for (decrement in fromEnd) {
    data[(index - decrement + 1):index, columnName] <- entries[length(entries)]
    entries <- entries[-length(entries)]
    index <- index - decrement
  }
  
  # now entries only contain one string
  # which is used to replace all NA entries
  data[is.na(data[[columnName]]),columnName] <- entries
  
  return(data)
}

#####################################################################
## Load data from cache
#####################################################################

load(file = file.path(dataDir, paste0("jobsRuntime-",  platform, ".Rda")))
load(file = file.path(dataDir, paste0("applicationsRuntimeExt-",  platform, ".Rda")))

# input checks
sum(!complete.cases(applicationsRuntimeExt))
Desc(applicationsRuntimeExt)

# column stageIds is a list and does not work with complete.cases and Desc
sum(!complete.cases(select(jobsRuntime, -stageIds)))
Desc(select(jobsRuntime, -stageIds))

#####################################################################
## Add step information 
#####################################################################

# All Random Forest Scenarios have exactly 30 jobs 
# that are spread across pipeline steps the following way:
# 0 - 4: Data Upload
# 5 - 8: Data Preparation
# 9 - 14: Pipeline Building
# 15 - 24: Model Building
# 25 - 26: Model Examination
# 27 - 29: Prediction

# Logistic Regression Scenarios have different number of job:
# 0 - 4: Data Upload
# 5 - 8: Data Preparation
# 9 - 14: Pipeline Building
# 15 - last-6: Model Building
# last-5 - last-3: Model Examination
# last-2 - last: Prediction

columnName = "step"
fromStart <- c(5, 4, 6) # from left to right
fromEnd <- c(3, 2) # starting from end
entries <- c("dataUpload", "dataPreparation", "pipelineBuilding",
             "modelBuilding", "modelExamination", "prediction")

jobsStepsRuntime <- split(jobsRuntime, jobsRuntime$id)
jobsStepsRuntime <- lapply(jobsStepsRuntime, addColumn, columnName = columnName,
                           fromStart = fromStart, fromEnd = fromEnd, entries = entries)

appNames <- names(jobsStepsRuntime)
appsBroken <- list()
for (i in 1:length(jobsStepsRuntime)) {
  if (sum(!complete.cases(select(jobsStepsRuntime[[i]], -stageIds))) != 0) {
    print(appNames[i]) # there is something wrong here
    appsBroken[[appNames[i]]] <- jobsStepsRuntime[[appNames[i]]]
  }
}
if(length(appsBroken) > 0) {stop("Incomplete Cases found in supplied data!!")}

jobsStepsRuntime <- bind_rows(jobsStepsRuntime)
sum(!complete.cases(select(jobsStepsRuntime, -stageIds)))

#####################################################################

# assign step information to each job and drop unnecessary information
dropCols <- c("submissionTime", "completionTime", "status", "numActiveTasks", "numActiveStages")
firstCols <- c("id", "appName", "step", "jobId", "jobName")
jobsStepsRuntime %<>%
  mutate(step = factor(step, levels = entries)) %>%
  select(-one_of(dropCols)) %>%
  select(one_of(firstCols), everything())
  
# this will calculate application runtime as total of all related jobs
appTotalRuntime <- jobsStepsRuntime %>%
  group_by_(.dots = c("id")) %>%
  summarize(appDuration = sum(duration))

sum(!complete.cases(appTotalRuntime))  

#####################################################################

dropCols <- c("startTime", "endTime", "lastUpdated")
firstCols <- c("id", "appName", "step", "stepDuration", "appDuration", "stepPerc")
stepsRuntime <- jobsStepsRuntime %>%
  group_by_(.dots = c("id", columnName)) %>%
  summarize(stepDuration = sum(duration)) %>%
  left_join(appTotalRuntime, by = c("id")) %>%
  left_join(select(applicationsRuntimeExt, -one_of(dropCols)), by = c("id")) %>%
  mutate(stepPerc = stepDuration / appDuration) %>%
  select(one_of(firstCols), everything()) %>%
  filter(!is.na(id)) # somehow we get NA cases, lets get rid of them

str(stepsRuntime)
Desc(stepsRuntime)

sum(!complete.cases(stepsRuntime))
stepsRuntime[!complete.cases(stepsRuntime),]
table(stepsRuntime$step)

#####################################################################

current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # not necessary
savewithBackup(stepsRuntime, 
               file = file.path(dataDir, paste0("stepsRuntime-", platform, ".Rda")))

####################################################################





####################################################################
## Adding phase information and continue processing
####################################################################
phaseLookupFile <- file.path(dataDir, "phaseLookupTable.csv")
phaseLookupTable <- fread(file = phaseLookupFile) # just to check...

entries <- c("data", "learning", "prediction")
allowedVars <- c("phase")
firstCols <- c("id", "appName", "phase", "phaseDuration", "appDuration", "phasePerc")
lookupTableFile <- file.path(dataDir, "phaseLookupTable.csv")

phasesRuntime <- 
  jobsStepsRuntime %>%
  select(-stageIds) %>%
  mutate(step = as.character(step)) %>% # addNewData only functions with characters 
  addNewData(lookupTableFile, allowedVars) %>%
  mutate(phase = factor(phase, levels = entries)) %>%
  group_by_(.dots = c("id", "phase")) %>%
  summarize(phaseDuration = sum(duration)) %>%
  left_join(appTotalRuntime, by = c("id")) %>%
  left_join(select(applicationsRuntimeExt, -one_of(dropCols)), by = c("id")) %>%
  mutate(phasePerc = phaseDuration / appDuration) %>%
  select(one_of(firstCols), everything()) 

str(phasesRuntime)
Desc(phasesRuntime)

sum(!complete.cases(phasesRuntime))
phasesRuntime[!complete.cases(phasesRuntime),]

#####################################################################

savewithBackup(phasesRuntime, 
               file = file.path(dataDir, paste0("phasesRuntime-", platform, ".Rda")))

####################################################################
