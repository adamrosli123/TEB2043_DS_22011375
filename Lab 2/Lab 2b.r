# ==========================================
# Activity 1: BMI Logical Categories
# ==========================================
cat("--- Activity 1 ---\n")
weight <- as.numeric(readline(prompt="Enter your weight (in kg): "))
height <- as.numeric(readline(prompt="Enter your height (in meters): "))
bmi <- weight / (height^2)

underweight <- bmi <= 18.4
normal <- bmi >= 18.5 & bmi <= 24.9
overweight <- bmi >= 25.0 & bmi <= 39.9
obese <- bmi >= 40.0

cat("Underweight:", underweight, "\n")
cat("Normal:", normal, "\n")
cat("Overweight:", overweight, "\n")
cat("Obese:", obese, "\n\n")

# ==========================================
# Activity 2: Case-Insensitive String Compare
# ==========================================
cat("--- Activity 2 ---\n")
str1 <- readline(prompt="Enter string 1: ")
str2 <- readline(prompt="Enter string 2: ")

is_similar <- tolower(str1) == tolower(str2)
cat("This program compare 2 strings. Both inputs are similar:", is_similar, "\n\n")

# ==========================================
# Activity 3: Name and Phone Manipulation
# ==========================================
cat("--- Activity 3 ---\n")
name <- readline(prompt="Enter your name: ")
phone <- readline(prompt="Enter your phone number (e.g., 01412348752): ")

name_upper <- toupper(name)
first_3 <- substr(phone, 1, 3)
last_4 <- substr(phone, nchar(phone)-4, nchar(phone)) # Adjusted to grab exactly the last 4 digits

cat("Hi,", name_upper, "\n")
cat("A verification code has been sent to", first_3, "-xxxxx", last_4, "\n")
