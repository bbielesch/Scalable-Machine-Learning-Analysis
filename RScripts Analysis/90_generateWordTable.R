#####################################################################
## 
## Output Word Tables
##
#####################################################################

# install.packages("rJava")
# devtools::install_github('davidgohel/ReporteRsjars')
# devtools::install_github('davidgohel/ReporteRs')

#####################################################################

rm(list = ls()) # clean global environment

includeDir <- "include"
source(file.path(includeDir, "includeRegular.R"))

require(ReporteRs) # this package is not universally necessary therefore not loaded in all scripts

cacheDir <- "cache" # general directory locations
dataDir <- "data"
plotDir <- "plot"
outputDir <- "output" # general output directory
options(scipen = 100000)

#####################################################################
##
## Tables for analysis of single-node cluster
##
#####################################################################

platform <- "Local" # could also be "Local" or "Scape02"

# load necessary files

load(file = file.path(dataDir, paste0("outputParamsExt-", platform, ".Rda")))
load(file = file.path(dataDir, paste0("stepsRuntime-", platform, ".Rda")))
load(file = file.path(dataDir, paste0("phasesRuntime-", platform, ".Rda")))

doc <- docx(title = 'Analysis Single-node cluster - Tables')
roundCols <- c("maxPerc", "minPerc", "meanPerc", "medianPerc", "sdPerc")
output <- 
  stepsRuntime %>%
  group_by_(.dots = c("step")) %>%
  summarize(maxPerc = max(stepPerc),
            minPerc = min(stepPerc),
            meanPerc = mean(stepPerc),
            medianPerc = median(stepPerc),
            sdPerc = sd(stepPerc)) %>%
  mutate_at(.vars = roundCols, .funs = funs(round(., 3))) %>%
  mutate(step = as.character(step))

doc = addFlexTable(doc, vanilla.table(output))
doc = addPageBreak(doc)

output <- 
  phasesRuntime %>%
  group_by_(.dots = c("phase")) %>%
  summarize(maxPerc = max(phasePerc),
            minPerc = min(phasePerc),
            meanPerc = mean(phasePerc),
            medianPerc = median(phasePerc),
            sdPerc = sd(phasePerc)) %>%
  mutate_at(.vars = roundCols, .funs = funs(round(., 3))) %>%
  mutate(phase = as.character(phase))

doc = addFlexTable(doc, vanilla.table(output))

fileName <- file.path(outputDir, paste0("analysis-", platform, ".docx"))
writeDoc(doc, fileName)

#####################################################################
##
## Tables for analysis of multi-node cluster
##
#####################################################################

platform = "Scape01"

# load necessary files
load(file = file.path(dataDir, paste0("outputParamsExt-", platform, ".Rda")))
load(file = file.path(dataDir, paste0("stepsRuntime-", platform, ".Rda")))

doc <- docx(title = 'Analysis Multi-node cluster - Tables')

# generate table and insert into document
roundCols <- c("maxPerc", "minPerc", "meanPerc", "medianPerc", "sdPerc")
output <- 
  stepsRuntime %>%
  group_by_(.dots = c("step")) %>%
  summarize(maxPerc = max(stepPerc),
            minPerc = min(stepPerc),
            meanPerc = mean(stepPerc),
            medianPerc = median(stepPerc),
            sdPerc = sd(stepPerc)) %>%
  mutate_at(.vars = roundCols, .funs = funs(round(., 3))) %>%
  mutate(step = as.character(step))

doc = addFlexTable(doc, vanilla.table(output))
doc = addPageBreak(doc)

roundCols <- c("maxPerc", "minPerc", "meanPerc", "medianPerc", "sdPerc")
output <- 
  phasesRuntime %>%
  group_by_(.dots = c("phase")) %>%
  summarize(maxPerc = max(phasePerc),
            minPerc = min(phasePerc),
            meanPerc = mean(phasePerc),
            medianPerc = median(phasePerc),
            sdPerc = sd(phasePerc)) %>%
  mutate_at(.vars = roundCols, .funs = funs(round(., 3))) %>%
  mutate(phase = as.character(phase))

doc = addFlexTable(doc, vanilla.table(output))
fileName <- file.path(outputDir, paste0("analysis-", platform, ".docx"))
writeDoc(doc, fileName)

#####################################################################

