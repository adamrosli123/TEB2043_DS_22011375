# ============================================================
# Question 1: Retirement Age Factor Analysis

# Create the age vector
age <- c(55,57,56,52,51,59,58,53,59,55,60,60,60,60,52,55,56,51,60,
         52,54,56,52,57,54,56,58,53,53,50,55,51,57,60,57,55,51,50,57,58)

# Convert to factor and display levels
age_factor <- factor(age)
cat("Levels of the age factor:\n")
print(levels(age_factor))

# Frequency table for individual ages
cat("\nFrequency table of Staff Age:\n")
age_table <- table(age_factor)
print(age_table)

# Divide the levels of factor into 5 ranges
age_ranges <- cut(age, breaks = c(49, 52, 54, 56, 58, 60),
                  labels = c("50-52", "52-54", "54-56", "56-58", "58-60"),
                  right = TRUE)

cat("\nFrequency table of Age Ranges:\n")
range_table <- table(age_ranges)
print(range_table)

# Conclusion / Insight
cat("\n--- Insight ---\n")
cat("From the frequency table, we can observe the distribution of retirement ages.\n")
cat("This helps identify which age ranges have the most retirements in Company A.\n")


# ============================================================
# Question 2: Matrix Operations using V1

# Create vector V1
V1 <- c(2, 3, 1, 5, 4, 6, 8, 7, 9)

# Create Matrix-1 (3x3 matrix, filled by column by default)
Matrix1 <- matrix(V1, nrow = 3, ncol = 3)
cat("\nMatrix-1:\n")
print(Matrix1)

# Transpose Matrix-1 to create Matrix-2
Matrix2 <- t(Matrix1)
cat("\nMatrix-2 (Transpose of Matrix-1):\n")
print(Matrix2)

# Rename rows and columns for Matrix-1
rownames(Matrix1) <- c("Row1", "Row2", "Row3")
colnames(Matrix1) <- c("Col1", "Col2", "Col3")
cat("\nMatrix-1 with renamed rows and columns:\n")
print(Matrix1)

# Rename rows and columns for Matrix-2
rownames(Matrix2) <- c("Row1", "Row2", "Row3")
colnames(Matrix2) <- c("Col1", "Col2", "Col3")
cat("\nMatrix-2 with renamed rows and columns:\n")
print(Matrix2)

# Add the matrices
cat("\nAddition of Matrix-1 and Matrix-2:\n")
print(Matrix1 + Matrix2)

# Subtract the matrices
cat("\nSubtraction of Matrix-1 and Matrix-2:\n")
print(Matrix1 - Matrix2)

# Multiply the matrices (element-wise)
cat("\nMultiplication of Matrix-1 and Matrix-2 (element-wise):\n")
print(Matrix1 * Matrix2)

# Divide the matrices (element-wise)
cat("\nDivision of Matrix-1 by Matrix-2 (element-wise):\n")
print(Matrix1 / Matrix2)


# ============================================================
# Question 3: Arrays

# Array 1: 4 columns, 2 rows, 3 tables
Array1 <- array(1:24, dim = c(2, 4, 3))
cat("\nArray1\n")
print(Array1)

# Array 2: 2 columns, 3 rows, 5 tables
Array2 <- array(25:54, dim = c(3, 2, 5))
cat("\nArray2\n")
print(Array2)

# Print the second row of the second matrix of the first array
cat("\n\"The second row of the second matrix of the array:\"\n")
print(Array1[2, , 2])

# Print the element in the 3rd row and 3rd column of the first matrix of the second array
cat("\"The element in the 3rd row and 3rd column of the 1st matrix:\"\n")
print(Array2[3, 2, 1])
