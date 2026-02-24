# ============================================
# Activity 1: Check whether a year is leap year or not

year <- as.integer(readline(prompt = "Input year: "))

if ((year %% 4 == 0 & year %% 100 != 0) | (year %% 400 == 0)) {
    print(paste(year, "is a leap year."))
} else {
    print(paste(year, "is not leap year."))
}

# ============================================
# Activity 2: Display the cube of numbers up to a given integer

n <- as.integer(readline(prompt = "Input an integer: "))

for (i in 1:n) {
    print(paste("Number is:", i, "and cube of the", i, "is :", i^3))
}

# ============================================
# Activity 3: Check Armstrong number of n digits

cat("Check whether an n digits number is Armstrong or not:\n")
cat("------------------------------------------------------\n")

num <- as.integer(readline(prompt = "Input an integer: "))

digits <- nchar(as.character(num))
temp <- num
sum <- 0

while (temp > 0) {
    digit <- temp %% 10
    sum <- sum + digit^3
    temp <- temp %/% 10
}

if (sum == num) {
    print(paste(num, "is an Armstrong number."))
} else {
    print(paste(num, "is not an Armstrong number."))
}
