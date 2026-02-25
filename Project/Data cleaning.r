library(tidyverse)
library(lubridate)

# 1. DATA INGESTION & PRE-PROCESSING
# Read the raw lines of the file as text
raw_lines <- read_lines("UncleanDataset.csv", locale = locale(encoding = "latin1"))

# Remove empty lines
raw_lines <- raw_lines[raw_lines != ""]

# Create an empty list to store the cleaned rows
parsed_rows <- list()

# Loop through each line (skipping the first row which is the header)
for (i in 2:length(raw_lines)) {
  line <- raw_lines[i]
  
  # Check if the row uses a pipe "|" delimiter
  if (str_detect(line, fixed("|"))) {
    # If it has a pipe and a trailing comma, we split by comma first
    # to grab just the actual data. Then split it by the pipe.
    clean_line <- str_split(line, ",")[[1]][1]
    fields <- str_split(clean_line, fixed("|"))[[1]]
  } else {
    # If it's a standard comma-delimited row, we read it safely as a CSV row
    fields <- as.character(read.csv(text = line, header = FALSE, stringsAsFactors = FALSE))
  }
  
  # Ensure every row has exactly 8 columns (pad with NA if short)
  length(fields) <- 8
  
  # Trim empty whitespace from each field and add to our list
  parsed_rows[[i - 1]] <- trimws(fields)
}

# Convert the parsed list into a proper Data Frame
target_cols <- c("Student_ID", "First_Name", "Last_Name", "Age", "Gender", 
                 "Course", "Enrollment_Date", "Total_Payments")

df_raw <- as.data.frame(do.call(rbind, parsed_rows), stringsAsFactors = FALSE)
colnames(df_raw) <- target_cols

# Convert all empty strings "" to NA so R treats them properly as missing data
df_raw <- df_raw %>% na_if("")

# 2. CONSOLIDATED CLEANING PIPELINE
df_clean <- df_raw %>%
  # A. Fix mixed-up Gender and Age (e.g., when Gender column accidentally says "M 25")
  mutate(
    # If Gender contains a number (\d), extract the number and move it to Age
    Age = if_else(str_detect(Gender, "\\d"), str_extract(Gender, "\\d+"), Age),
    # If Gender contains a number, keep only the 1st letter (e.g., "M" from "M 25")
    Gender = if_else(str_detect(Gender, "\\d"), str_sub(Gender, 1, 1), Gender)
  ) %>%
  
  # B. Fix Names (Missing Last Names or First Name containing both names)
  mutate(
    # If First and Last name are identical duplicates, set Last Name to NA
    Last_Name = if_else(First_Name == Last_Name, NA_character_, Last_Name),
    
    # If Last Name is missing and First Name has a space, extract the 2nd word to Last Name
    Last_Name = if_else(is.na(Last_Name) & str_detect(First_Name, " "), word(First_Name, 2, -1), Last_Name),
    # And keep only the 1st word in First Name
    First_Name = if_else(str_detect(First_Name, " "), word(First_Name, 1), First_Name),
    
    # Standardize capitalization (e.g., "john" -> "John")
    First_Name = str_to_title(First_Name),
    Last_Name = str_to_title(Last_Name)
  ) %>%
  
  # C. Clean Numeric & Date Columns
  mutate(
    # Keep only numbers [0-9] in Student_ID and Age
    Student_ID = as.integer(str_remove_all(Student_ID, "[^0-9]")),
    Age = as.integer(str_remove_all(Age, "[^0-9]")),
    
    # Cap age to realistic values (between 15 and 70), otherwise set to NA
    Age = if_else(Age >= 15 & Age <= 70, Age, NA_integer_),
    
    # Capitalize Gender and restrict to just 'M' or 'F'
    Gender = toupper(Gender),
    Gender = if_else(Gender %in% c("M", "F"), Gender, NA_character_),
    
    # Remove dollar signs/symbols and convert payments to numerical format
    Total_Payments = as.numeric(str_remove_all(Total_Payments, "[^0-9.]")),
    
    # Parse dates flexibly handling various formats (Year-Month-Day, Day-Month-Year, etc.)
    Enrollment_Date = parse_date_time(Enrollment_Date, orders = c("Ymd", "dmy", "dmY", "d-b-y"), quiet = TRUE),
    Enrollment_Date = as.Date(Enrollment_Date),
    
    # Reject futuristic or incredibly old garbage dates
    Enrollment_Date = if_else(year(Enrollment_Date) >= 2000 & year(Enrollment_Date) <= year(Sys.Date()), 
                              Enrollment_Date, as.Date(NA))
  ) %>%
  
  # D. Standardize Course Names
  mutate(
    Course = str_to_title(trimws(Course)),
    Course = case_when(
      str_detect(Course, "Machine Learn") ~ "Machine Learning",
      str_detect(Course, "Web Dev") ~ "Web Development",
      str_detect(Course, "Data Sci") ~ "Data Science",
      str_detect(Course, "Data Anal") ~ "Data Analysis",
      str_detect(Course, "Cyber") ~ "Cyber Security",
      Course == "4" ~ NA_character_, # Remove garbage values
      TRUE ~ Course
    )
  )

# 3. REMOVE DUPLICATES & EMPTY ROWS
df_clean <- df_clean %>%
  # Remove rows that are entirely blank in the key identification columns
  filter(!(is.na(Student_ID) & is.na(First_Name) & is.na(Last_Name) & is.na(Age))) %>%
  distinct() %>% # Remove exact duplicate rows
  group_by(Student_ID) %>%
  slice(1) %>% # Keep only the first occurrence of each Student ID
  ungroup()

# 4. FIX MISSING IDs & OUTLIERS
# Generate consecutive unique IDs for rows missing a Student ID
if (any(is.na(df_clean$Student_ID))) {
  missing_count <- sum(is.na(df_clean$Student_ID))
  next_id <- max(df_clean$Student_ID, na.rm = TRUE) + 1
  df_clean$Student_ID[is.na(df_clean$Student_ID)] <- seq(next_id, length.out = missing_count)
}

# Find the statistical limit for Payments (Mean + 3 Standard Deviations)
payment_mean <- mean(df_clean$Total_Payments, na.rm = TRUE)
payment_sd <- sd(df_clean$Total_Payments, na.rm = TRUE)
upper_limit <- payment_mean + (3 * payment_sd)

df_clean <- df_clean %>%
  mutate(
    # Cap insane payment outliers to our upper limit
    Total_Payments = if_else(Total_Payments > upper_limit, upper_limit, Total_Payments)
  )

# 5. FINAL IMPUTATION (Filling in missing values)
# A simple custom function to calculate the Mode (most frequent value) for categorical data
get_mode <- function(v) {
  v <- na.omit(v)
  if (length(v) == 0) return(NA)
  uniq_v <- unique(v)
  uniq_v[which.max(tabulate(match(v, uniq_v)))]
}

df_final <- df_clean %>%
  mutate(
    Age = replace_na(Age, as.integer(round(mean(Age, na.rm = TRUE)))),
    Gender = replace_na(Gender, get_mode(Gender)),
    Course = replace_na(Course, get_mode(Course)),
    Enrollment_Date = replace_na(Enrollment_Date, median(Enrollment_Date, na.rm = TRUE)),
    Total_Payments = replace_na(Total_Payments, median(Total_Payments, na.rm = TRUE))
  ) %>%
  arrange(Student_ID) # Sort the dataset neatly by ID at the end

# 6. EXPORT
write_csv(df_final, "CleanedDataset.csv")
