#####################################################################
##
## Extract information from output files of Spark model building runs
##
#####################################################################

rm(list = ls()) # clean global environment

includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

platform <- "Scape01" # could also be "Local" or "Scape02" - change accordingly before execution
testSeries <- "RF-Part2" # also change as needed

outputParamsEnv <- new.env()

#####################################################################
# some basic parameters - directory location
# adapt accordingly
inputDir <- file.path("/media", "berni", "Studium", "MasterThesis")

# define name for test series
# outputSubDir <- file.path(platform, testSeries, "output-stdout")
outputSubDir <- file.path("Scape01-TO_BE_PROCESSED", "stdout-rf")
# retrieve directory information
outputFiles <- list.files(file.path(inputDir, outputSubDir))
# character vectors only contain file names but no path information

# initialize empty data.table
outputParamsEnv[[testSeries]] <- data.table(appName = character(), 
                                            configFile = character(),
                                            featureFile = character(),
                                            labelFile = character(),
                                            splitFile = character(),
                                            trainObservations = double(),
                                            testObservations = double(),
                                            testError = double(),
                                            stringsAsFactors=FALSE)

#####################################################################
## Retrieve all relevant information present in output files
#####################################################################

for (outputFile in outputFiles) {
  
  # empty all content from previous run - just to make sure
  appName <- configFile <- featureFile <- labelFile <- splitFile <- ""
  trainObservations <- testObservations <- testError = 0
  
  outputFileContent <- readLines(file.path(inputDir, outputSubDir, outputFile))
  
  for (outputLine in outputFileContent) {
    # each line can only match with one of the following patterns
    if (str_detect(outputLine, pattern = regex("(?<=Application name: ).*")))
      appName <- str_extract(outputLine, pattern = regex("(?<=Application name: ).*"))
    
    if (str_detect(outputLine, pattern = regex("(?<=configuration file: ).*")))
      configFile <- basename(str_extract(outputLine, pattern = regex("(?<=configuration file: ).*")))
    
    if (str_detect(outputLine, pattern = regex("(?<=Features file: ).*")))
      featureFile <- basename(str_extract(outputLine, pattern = regex("(?<=Features file: ).*")))
    
    if (str_detect(outputLine, pattern = regex("(?<=Labels file: ).*")))
      labelFile <- basename(str_extract(outputLine, pattern = regex("(?<=Labels file: ).*")))
    
    if (str_detect(outputLine, pattern = regex("(?<=Splits file: ).*")))
      splitFile <- basename(str_extract(outputLine, pattern = regex("(?<=Splits file: ).*")))
    
    if (str_detect(outputLine, pattern = regex("(?<=Training Dataset: ).*")))
      trainObservations <- as.numeric(str_extract(outputLine, pattern = regex("(?<=Training Dataset: ).*")))
    
    if (str_detect(outputLine, pattern = regex("(?<=Testing Dataset: ).*")))  
      testObservations <- as.numeric(str_extract(outputLine, pattern = regex("(?<=Testing Dataset: ).*")))
    
    if (str_detect(outputLine, pattern = regex("(?<=Test Error = ).*")))  
      testError <- as.numeric(str_extract(outputLine, pattern = regex("(?<=Test Error = ).*")))
  } # end for (outputLine in output)
  
  outputParamsEnv[[testSeries]] <- rbindlist(list(outputParamsEnv[[testSeries]],
                                               list(appName,
                                                    configFile, 
                                                    featureFile,
                                                    labelFile,
                                                    splitFile,
                                                    trainObservations,
                                                    testObservations,
                                                    testError)))
  
} # end for (outputFile in outputFiles)

#####################################################################
## Store compiled information as Rda
#####################################################################

current <- format(Sys.time(), "%Y-%m-%d_%H-%M-%S") # currently not used
savewithBackup(list = c(testSeries), 
               file = file.path(cacheDir, paste0("outputParams-", platform, "-",  testSeries, ".Rda")),
               envir = outputParamsEnv)

####################################################################




####################################################################
# load("cache/outputParams-Scape01-percentageSplit_0.9_cores1.Rda", envir = outputParamsEnv)
# outputParamsEnv[["percentageSplit_0.9-cores1"]] <- 
#   outputParamsEnv[["percentageSplit_0.9_cores1"]]
# ls(outputParamsEnv)
# save(list = c("percentageSplit_0.9-cores1"), 
#      file = "cache/outputParams-Scape01-percentageSplit_0.9-cores1.Rda", 
#      envir = outputParamsEnv)
#####################################################################
