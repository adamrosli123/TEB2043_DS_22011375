# Activity

# 1. Store the table as a data frame
cat("===== Q1: Create Data Frame =====\n")
exam <- data.frame(
  name = c("Anastasia", "Dima", "Michael", "Matthew", "Laura", "Kevin", "Jonas"),
  score = c(12.5, 9.0, 16.5, 12.0, 9.0, 8.0, 19.0),
  attempts = c(1, 3, 2, 3, 2, 1, 2)
)
print(exam)

# 2. Add new column "qualify"
cat("\n===== Q2: Add 'qualify' Column =====\n")
exam$qualify <- c("yes", "no", "yes", "no", "no", "no", "yes")
print(exam)

# 3. Add new row (Emily)
cat("\n===== Q3: Add New Row (Emily) =====\n")
new_row <- data.frame(
  name = "Emily",
  score = 14.5,
  attempts = 1,
  qualify = "yes"
)
exam <- rbind(exam, new_row)
print(exam)

# 4. Display structure, summary, and dimensions
cat("\n===== Q4: Structure =====\n")
str(exam)

cat("\n===== Q4: Summary (with corrected types) =====\n")
# Change column data types for a more meaningful summary
exam$name <- as.factor(exam$name)
exam$qualify <- as.factor(exam$qualify)
exam$attempts <- as.integer(exam$attempts)
print(summary(exam))

cat("\n===== Q4: Dimensions =====\n")
cat("Rows:   ", nrow(exam), "\n")
cat("Columns:", ncol(exam), "\n")
cat("Dim:    ", dim(exam), "\n")
