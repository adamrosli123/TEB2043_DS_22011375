# ==========================================
# Activity 1: Number Sequence and Squares
# ==========================================
cat("--- Activity 1 ---\n")
numbers <- 1:20
squares <- numbers^2
result <- data.frame(Number = numbers, Square = squares)
print(result)
cat("\n")

# ==========================================
# Activity 2: Decimal Rounding
# ==========================================
cat("--- Activity 2 ---\n")
num1 <- 0.956786
num2 <- 7.8345901

cat("num1:", round(num1, 2), "\n")
cat("num2:", round(num2, 3), "\n\n")

# ==========================================
# Activity 3: Circle Area Calculator
# ==========================================
cat("--- Activity 3 ---\n")
radius_input <- readline(prompt="Enter the radius of the circle: ")
radius <- as.numeric(radius_input)
area <- pi * (radius^2)
cat("The circle's area is:", area, "\n")
