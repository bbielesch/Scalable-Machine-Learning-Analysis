#####################################################################
## MSD Data Preparation Tasks
#####################################################################

#####################################################################
## Convert ARFF files to CSV files without further checks
#####################################################################

path <- file.path("/media", "bernhard", "Studium", "MSD", "MSD-TU", "CompleteFeatures", "jMir")
files <- list.files(path = path, full.names = TRUE, pattern = "\\.arff$")

for (f in files) {
  tryCatch ({data <- readARFF(f)},
            error = function(e) {
              message(paste0("Error download file ", f))
            })
  
  newName <- paste0(str_sub(f, end = -5), "csv")
  fwrite(data, file = newName, showProgress = TRUE)
  rm(data) # remove data to make space for next data file
}

#####################################################################
## Examine CSV files
#####################################################################

featuresPath <- file.path("/media", "bernhard", "Studium", "MSD", "MSD-TU", "features")
featuresFiles <- list.files(path = featuresPath, full.names = TRUE, pattern = "\\.csv$")

for (file in featuresFiles) {
  data <- fread(file, nrow = 0)
  print(paste0(file, ": ", ncol(data)))
  rm(data)
}

#####################################################################
# Read TXT file for tracks per year and convert to CSV file
#####################################################################

raw <- readLines(file = file.path("/media", "bernhard", "Studium", "MSD", "MSD", "tracks_per_year.txt")
tracks <- as.data.frame(raw) %>% 
  separate(raw, into = c("year", "MSD_TRACKID", "artist", "song"), sep = "<SEP>") %>%
  transform(year = as.numeric(year))

str(tracks) # should be fine
fwrite(tracks, file = file.path("media", "bernhard", "Studium", "MSD", "MSD", "msd_tracks_per_year.csv")
#####################################################################
