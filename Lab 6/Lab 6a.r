#1.1

emp.data <- data.frame(
  emp_id = c(1:5),
  emp_name = c("Rick","Dan","Michelle", "Ryan", "Gary"),
  salary = c(623.3, 515.2, 611.0, 729.0, 843.23)
)

#1.2
cat("\n===== 1.2 Matrix =====\n")
my_matrix <- matrix(c(1, 2, 3, 4, 5, 6), nrow=2)
print(my_matrix)

cat("\n===== 1.2 Data Frame from Matrix =====\n")
df_from_matrix <- data.frame(my_matrix)
print(df_from_matrix)

cat("\n===== 1.2 Renamed Columns =====\n")
names(df_from_matrix) <- c('col_1', 'col_2', 'col_3')
print(df_from_matrix)

#1.3
cat("\n===== 1.3 List =====\n")
my_list <- list(rating=1:4,
animal=c('koala', 'hedgehog', 'sloth', 'panda'),
country=c('Australia', 'Italy', 'Peru', 'China'),
avg_sleep_hours=c(21, 18, 17, 10))
print(my_list)

cat("\n===== 1.3 Super Sleepers Data Frame =====\n")
super_sleepers <- data.frame(my_list)
print(super_sleepers)


#2.1
cat("\n===== 2.1 Employee Data =====\n")
print(emp.data)


#2.2
cat("\n===== 2.2 Structure =====\n")
str(emp.data)

#2.3
cat("\n===== 2.3 Summary =====\n")
print(summary(emp.data))

#2.4
cat("\n===== 2.4 Dimensions =====\n")
cat("dim:  "); print(dim(emp.data))
cat("ncol: "); print(ncol(emp.data))
cat("nrow: "); print(nrow(emp.data))

#3.1
cat("\n===== 3.1 Accessing Columns =====\n")
cat("emp.data$salary:\n")
print(emp.data$salary)
cat('emp.data[["salary"]]:\n')
print(emp.data[["salary"]])
cat("emp.data[3]:\n")
print(emp.data[3])

#3.2
cat("\n===== 3.2 Extracting Rows 1:2 =====\n")
result <- emp.data[1:2,]
print(result)

#4.1
cat("\n===== 4.1 Add dept Column =====\n")
emp.data$dept <- c("IT","Operations","IT","HR","Finance")
v <- emp.data
print(v)

cat("\n===== 4.1 Add start_date Column (cbind) =====\n")
emp.newdata<- cbind(emp.data,
start_date = as.Date(c("2012-01-01", "2013-09-23", "2014-11-15", "2014-05-11","2015-03-27")))
print(emp.newdata)

#4.2
cat("\n===== 4.2 Bind Two Data Frames (rbind) =====\n")
emp.newdata <- data.frame(
emp_id = c (6:8),
emp_name = c("Rasmi","Pranab","Tusar"),
salary = c(578.0,722.5,632.8),
dept = c("IT","Operations","Finance")
)
emp.finaldata <- rbind(emp.data,emp.newdata)
print(emp.finaldata)

#4.3
cat("\n===== 4.3 Remove the second row and first column =====\n")
result <- emp.data [-c(2), -c(1)]
print(result)

#5 Extra: Create friends data frame and modify its structure
cat("\n===== 5. Friends Data Frame =====\n")
friends <- data.frame(
  friend_id = c(1:5),
  friend_name = c("Sachin", "Sourav", "Dravid", "Sehwag", "Dhoni"),
  location = c("Kolkata", "Delhi", "Bangalore", "Hyderabad", "Chennai")
)
print(friends)

# Add columns with various data types
cat("\n===== 5. Add Columns (int, date, logical, numeric) =====\n")
friends$age <- as.integer(c(52, 51, 50, 45, 42))
friends$joined_date <- as.Date(c("1989-11-15", "1992-01-03", "1996-10-20", "1999-04-01", "2004-12-23"))
friends$is_active <- c(FALSE, FALSE, FALSE, FALSE, FALSE)
friends$score <- c(98.5, 87.3, 92.1, 88.7, 95.0)
print(friends)

# Check structure to confirm various data types
cat("\n===== 5. Structure (showing data types) =====\n")
str(friends)

# Add more rows
cat("\n===== 5. Add New Rows (rbind) =====\n")
new_friends <- data.frame(
  friend_id = c(6, 7),
  friend_name = c("Kohli", "Rohit"),
  location = c("Mumbai", "Mumbai"),
  age = as.integer(c(36, 37)),
  joined_date = as.Date(c("2008-08-18", "2007-06-23")),
  is_active = c(TRUE, TRUE),
  score = c(97.2, 93.8)
)
friends <- rbind(friends, new_friends)
print(friends)

