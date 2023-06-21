# R setup ####

#Clear environment#
rm(list = ls())

#Start time for script#
start_time <- Sys.time()

#Load in and/or install necessary packages# 
pacman::p_load(data.table, purrr, tictoc,
               httr, visdat, naniar, stringr,
               here,dbplyr, Rcpp, DBI, odbc, packrat,
               writexl,tidyverse,lubridate,
               cowplot,flextable,gtsummary,janitor,forcats, 
               highr,rstudioapi,keyring)

# get today's date
today <- Sys.Date()

# create folder name with today's date
folder_name <- format(today, "%Y-%m-%d")


saving_path <- paste0("C:/Users/", Sys.info()[[6]],"/Saving_folder")

tables <- c("table_1", "table_2", "table_3")

# Function to download tables ####
download_table <- function(table_name, con, saving_path, folder_name) {
  if (table_name == "table_1") {
    table_1 <- tbl(con, in_schema('schema1', 'Table 1')) %>%
      as_tibble() %>% select('Joining Date', 'Leaving Date', 'Salary','Retainer (Yes/No)') %>%  collect() %>% unique()
    fwrite(table_1, file.path(saving_path, folder_name, "table_1.csv"))
    table_1 <- read.csv(file.path(saving_path, folder_name, "/table_1.csv"))
    colnames(table_1) <- gsub("\\.", " ", colnames(table_1))
  } else if (table_name == "table_2") {
    table_2 <- tbl(con, in_schema('schema2', 'Table 2')) %>%
      as_tibble() %>% select(- 'Document ID', - 'Document Creation Date',) %>% collect() %>% unique()
    fwrite(table_2, file.path(saving_path, folder_name, "table_2.csv"))
    table_2 <- read.csv(file.path(saving_path, folder_name, "/table_2.csv"))
    colnames(table_2) <- gsub("\\.", " ", colnames(table_2))
  } else if (table_name == "table_3") {
    table_3 <- tbl(con, in_schema('schema2', 'Table 3')) %>%
      as_tibble() %>% collect() %>% unique()
    filtered_table_3 <- table_3 %>%
      filter(grepl("Record completed", Notifications))
    filtered_table_3 <- filtered_table_3[!duplicated(filtered_table_3$`Number`), ]
    table_3 <- filtered_table_3 
    fwrite(table_3, file.path(saving_path, folder_name, "table_3.csv"))
    table_3 <- read.csv(file.path(saving_path, folder_name, "/table_3.csv"))
    colnames(table_3) <- gsub("\\.", " ", colnames(table_3))
  }
}


# Data Import ####

# Check if folder exists for today's date
if (dir.exists(file.path(saving_path, folder_name))) {
  # If folder exists, check if all tables are already downloaded
  # Missing tables code #####
  missing_tables <- character(0)
  
  for (table_name in tables) {
    table_path <- file.path(saving_path, folder_name, paste0(table_name, ".csv"))
    if (!file.exists(table_path)) {
      missing_tables <- c(missing_tables, table_name)  # Add missing table to the list
    }
  }
  
  if (length(missing_tables) > 0) {
    # Download missing tables
    
    print("A table is missing from today's folder, connection to reporting database needed, please enter the password")
    
    # Define the connection to the SQL database
    con <- DBI::dbConnect(odbc::odbc(),
                          driver = "Driver Name",
                          Database = "Database",
                          UID = Sys.info()[[7]],
                          PWD = rstudioapi::askForPassword("Database password"),
                          Server = "Severname",
                          port = 01)
    
    download_times <- data.frame(Table = character(0), Time = character(0), stringsAsFactors = FALSE)  # Create an empty data frame
    
    missing_table_download_times <- data.frame(Table = character(0), Time = character(0), stringsAsFactors = FALSE)  # Create an empty data frame
    
    for (missing_table in missing_tables) {
      print(paste("Missing table:", missing_table))
      print("Downloading table...")
      
      # Measure download time
      tic()
      download_table(missing_table, con, saving_path, folder_name)
      toc_time <- toc()
      
      #Make a missing table download dataframe####
      # Add download time to the data frame
      missing_table_download_times<- rbind(missing_table_download_times, data.frame(Table = missing_table, Time = as.character(toc_time), stringsAsFactors = FALSE))
    }
    
    # Close the database connection
    DBI::dbDisconnect(con)
    rm(con)
    
    #Edit missing tables download times
    if (exists("missing_table_download_times") & nrow(missing_table_download_times) > 0) {
      missing_table_download_times <- missing_table_download_times[grep("sec elapsed", missing_table_download_times$Time), ]
      row.names(missing_table_download_times) <- NULL
    }
    
    
    # Save the missing tables download times if the data frame is not empty
    if (exists("missing_table_download_times") & nrow(missing_table_download_times) > 0) {
      print("Making Missing Table(s) Download Times csv")
      fwrite(missing_table_download_times, file.path(saving_path,folder_name,"/missing_table_download_times.csv"))
    }
    
    
    # Print the missing table download times if the data frame is not empty
    if (exists("missing_table_download_times") & nrow(missing_table_download_times) > 0) {
      print("Missing Table(s) Download Times:")
      print(missing_table_download_times)
    }
  } else {
    print("All tables already downloaded today - reading files from today's folder in General CRO Data")
  }
  
  # Load all tables
  # Load existing tables using read.csv
  table_1 <- read.csv(file.path(saving_path, folder_name, "/table_1.csv"))
  colnames(table_1) <- gsub("\\.", " ", colnames(table_1))
  
  table_2 <- read.csv(file.path(saving_path, folder_name, "/table_2.csv"))
  colnames(table_2) <- gsub("\\.", " ", colnames(table_2))
  
  table_3 <- read.csv(file.path(saving_path, folder_name, "/table_3.csv"))
  colnames(table_3) <- gsub("\\.", " ", colnames(table_3))

  
} else {
  # If folder does not exist, run data import code chunk####
  print("Running data import code chunk as folder with today's date does not exist.")
  
  # Create folder with today's date
  dir.create(file.path(saving_path, folder_name))
  
  ## Importing different tables necessary for analysis
  
  # Define the connection to the SQL database
  con <- DBI::dbConnect(odbc::odbc(),
                        driver = "Driver Name",
                        Database = "Database",
                        UID = Sys.info()[[7]],
                        PWD = rstudioapi::askForPassword("Database password"),
                        Server = "Severname",
                        port = 01)
  
  download_times <- data.frame(Table = character(0), Time = character(0), stringsAsFactors = FALSE)  # Create an empty data frame
  
  print("Downloading and loading table_1 table...")
  
  # Measure download time
  tic()
  download_table("table_1", con, saving_path, folder_name)
  toc_time <- toc()
  
  # Add download time to the data frame
  download_times <- rbind(download_times, data.frame(Table = "table_1", Time = as.character(toc_time), stringsAsFactors = FALSE))
  
  table_1 <- read.csv(file.path(saving_path, folder_name, "table_1.csv"))
  colnames(table_1) <- gsub("\\.", " ", colnames(table_1))
  
  print("Downloading and loading table_2 table...")
  
  # Measure download time
  tic()
  download_table("table_2", con, saving_path, folder_name)
  toc_time <- toc()
  
  # Add download time to the data frame
  download_times <- rbind(download_times, data.frame(Table = "table_2", Time = as.character(toc_time), stringsAsFactors = FALSE))
  
  table_2 <- read.csv(file.path(saving_path, folder_name, "table_2.csv"))
  colnames(table_2) <- gsub("\\.", " ", colnames(table_2))
  
  print("Downloading and loading table_3 table...")
  
  # Measure download time
  tic()
  download_table("table_3", con, saving_path, folder_name)
  toc_time <- toc()
  
  # Add download time to the data frame
  download_times <- rbind(download_times, data.frame(Table = "table_3", Time = as.character(toc_time), stringsAsFactors = FALSE))
  
  table_3 <- read.csv(file.path(saving_path, folder_name, "table_3.csv"))
  colnames(table_3) <- gsub("\\.", " ", colnames(table_3))
  

  # Close the database connection
  DBI::dbDisconnect(con)
  rm(con)
  
}


#Modify download time df to only include time elapsed
if (exists("download_times")) {
  download_times <- download_times[grep("sec elapsed", download_times$Time), ]
  row.names(download_times) <- NULL
}

# Save the download times if the data frame is not empty####
# Check if dataframe 'download_times' exists and meets another condition
if (exists("download_times") && nrow(download_times) > 0) {
  # Perform actions if 'download_times' exists and meets the additional condition
  print("Making Download Times csv")
  fwrite(download_times, file.path(saving_path, folder_name, "/download_times.csv"))
} 


# Print the download times if the data frame is not empty
if (exists("download_times") && nrow(download_times) > 0) {
  print("Download Times:")
  print(download_times)
}


# Create a data frame with script name and elapsed time
script_name <- "0_data_import_manipulation.Rmd"  # Replace with your script name
data <- data.frame("Script Name" = script_name, "Elapsed Time (Minutes)" = elapsed_time, "Date and Time" = format(start_time, "%Y-%m-%d %H:%M:%S"))

data$download <- NULL

# Check if missing_table_download_times dataframe exists and has at least 1 row
if (exists("missing_table_download_times") && nrow(missing_table_download_times) > 0) {
  data$Download <- "Missing tables downloaded"
} else {
  # Check if download_times dataframe exists and has at least 1 row
  if (exists("download_times") && nrow(download_times) > 0) {
    data$Download <- "Full Download"
  } else {
    data$Download <- "No Download"
  }
}



# Saving script timings####

# Create a data frame with script name and elapsed time
script_name <- "Example SQL Data Import Script"  # Replace with your script name
data <- data.frame("Script Name" = script_name, "Elapsed Time (Minutes)" = elapsed_time, "Date and Time" = format(start_time, "%Y-%m-%d %H:%M:%S"))

data$download <- NULL

# Check if missing_table_download_times dataframe exists and has at least 1 row
if (exists("missing_table_download_times") && nrow(missing_table_download_times) > 0) {
  data$Download <- "Missing tables downloaded"
} else {
  # Check if download_times dataframe exists and has at least 1 row
  if (exists("download_times") && nrow(download_times) > 0) {
    data$Download <- "Full Download"
  } else {
    data$Download <- "No Download"
  }
}

# Check if the CSV file exists
csv_file <- paste0("C:/Users/", Sys.info()[[6]],"/Savingpath/run_times.csv")

data <- data %>% dplyr::rename("Date and Time" = "Date.and.Time")

data <- data %>% dplyr::rename("Elapsed Time (Minutes)" = "Elapsed.Time..Minutes.")

data <- data %>% dplyr::rename("Script Name" = "Script.Name")



if (file.exists(csv_file)) {
  # If the file exists, append the data to it
  existing_data <- read.csv(csv_file, stringsAsFactors = FALSE)
  existing_data <- existing_data %>% dplyr::rename("Date and Time" = "Date.and.Time")
  existing_data <- existing_data %>% dplyr::rename("Elapsed Time (Minutes)" = "Elapsed.Time..Minutes.")
  existing_data <- existing_data %>% dplyr::rename("Script Name" = "Script.Name")
  updated_data <- rbind(existing_data, data)
  write.csv(updated_data, file = csv_file, row.names = FALSE, quote = FALSE)
} else {
  # If the file doesn't exist, create a new file with the data
  write.csv(data, file = csv_file, row.names = FALSE, quote = FALSE)
}

