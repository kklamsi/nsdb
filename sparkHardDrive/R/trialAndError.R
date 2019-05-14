library(sparklyr)
library(tidyverse)
library(dplyr)

sc <- spark_connect(master = "local")

read.csv.zip <- function(zipfile, ...) {
  # Create a name for the dir where we'll unzip
  zipdir <- tempfile()

  # Create the dir using that name
  dir.create(zipdir)
  # Unzip the file into the dir
  unzip(zipfile, exdir=zipdir)
  # Get a list of csv files in the dir
  files <- list.files(zipdir)
  files <- files[grep("//.csv$", files)]
  # Create a list of the imported csv files
  csv.data <- sapply(files, function(f) {
    fp <- file.path(zipdir, f)
    return(read.csv(fp, ...))
  })
  return(csv.data)}


data_Q1_2016 <- read.csv.zip("/data_Q1_2016.zip")
data_Q1_2016 <- read.csv("/data_Q1_2016.csv")

## harddriveKaggle ##

harddriveKaggle <- read.csv("/harddriveKaggle.csv")
harddriveKaggle <- select(harddriveKaggle, failure, smart_1_normalized, smart_3_normalized,
                          smart_5_normalized, smart_7_normalized, smart_9_normalized,
                          smart_192_normalized, smart_193_normalized, smart_194_normalized)


sampleFailure <- sample_n(filter(harddriveKaggle, failure == 1), 1000, replace = T)
sampleRunning <- sample_n(filter(harddriveKaggle, failure == 0), 1000, replace = F)

sample <- bind_rows(sampleFailure, sampleRunning)
