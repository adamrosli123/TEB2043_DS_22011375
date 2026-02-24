# Question 1: Store 20 exam scores, count grades, check pass/fail

# Input vector of 20 exam scores
scores <- c(33, 24, 54, 94, 16, 89, 60, 6, 77, 61, 13, 44, 26, 24, 73, 73, 90, 39, 90, 54)

# Count number of students based on grade
grade_A <- sum(scores >= 90 & scores <= 100)  # 90-100
grade_B <- sum(scores >= 80 & scores <= 89)   # 80-89
grade_C <- sum(scores >= 70 & scores <= 79)   # 70-79
grade_D <- sum(scores >= 60 & scores <= 69)   # 60-69
grade_E <- sum(scores >= 50 & scores <= 59)   # 50-59
grade_F <- sum(scores <= 49)                   # <=49

cat("=== Question 1 ===\n")
cat("Exam Scores:", scores, "\n\n")

cat("Grade Distribution:\n")
cat("Grade A (90-100):", grade_A, "students\n")
cat("Grade B (80-89):", grade_B, "students\n")
cat("Grade C (70-79):", grade_C, "students\n")
cat("Grade D (60-69):", grade_D, "students\n")
cat("Grade E (50-59):", grade_E, "students\n")
cat("Grade F (<=49):", grade_F, "students\n\n")

# Check whether each student pass the exam (>49) or not. Return TRUE or FALSE.
pass_status <- scores > 49
cat("Pass Status (>49):\n")
print(pass_status)
cat("\n")

# Question 2: Store student record in a list with names, find highest, lowest, and average of exam score

# Create a named list of student exam scores
student_scores <- list(
  Robert = 59,
  Hemsworth = 71,
  Scarlett = 83,
  Evans = 68,
  Pratt = 65,
  Larson = 57,
  Holland = 62,
  Paul = 92,
  Simu = 92,
  Renner = 59
)

cat("\n=== Question 2 ===\n")
cat("Student Records:\n")
print(student_scores)

# Convert list to vector for calculations
score_vector <- unlist(student_scores)

# Find highest, lowest, and average
highest_score <- max(score_vector)
lowest_score <- min(score_vector)
average_score <- mean(score_vector)

# Find student(s) with highest and lowest score
student_highest <- names(which(score_vector == highest_score))
student_lowest <- names(which(score_vector == lowest_score))

cat("\nHighest Score:", highest_score, "\n")
cat("Lowest Score:", lowest_score, "\n")
cat("Average Score:", average_score, "\n")
cat("Student with highest score:", student_highest, "\n")
cat("Student with lowest score:", student_lowest, "\n")

# Question 3: Append Chemistry and Physics scores, count failures, find highest/best scores

cat("\n\n=== Question 3 ===\n")

# Append Chemistry and Physics scores to the student record
student_scores$Robert <- list(ExamScore = 59, Chemistry = 59, Physics = 89)
student_scores$Hemsworth <- list(ExamScore = 71, Chemistry = 71, Physics = 86)
student_scores$Scarlett <- list(ExamScore = 83, Chemistry = 83, Physics = 65)
student_scores$Evans <- list(ExamScore = 68, Chemistry = 68, Physics = 52)
student_scores$Pratt <- list(ExamScore = 65, Chemistry = 65, Physics = 60)
student_scores$Larson <- list(ExamScore = 57, Chemistry = 57, Physics = 67)
student_scores$Holland <- list(ExamScore = 62, Chemistry = 62, Physics = 40)
student_scores$Paul <- list(ExamScore = 92, Chemistry = 92, Physics = 77)
student_scores$Simu <- list(ExamScore = 92, Chemistry = 92, Physics = 90)
student_scores$Renner <- list(ExamScore = 59, Chemistry = 59, Physics = 61)

cat("Updated Student Records (with Chemistry & Physics):\n")
print(student_scores)

# Extract Chemistry and Physics scores into vectors
chemistry_scores <- sapply(student_scores, function(x) x$Chemistry)
physics_scores <- sapply(student_scores, function(x) x$Physics)
student_names <- names(student_scores)

# Count students who fail Chemistry (<=49)
fail_chemistry <- student_names[chemistry_scores <= 49]
cat("\nStudents who failed Chemistry (<=49):", 
    if(length(fail_chemistry) == 0) "None" else fail_chemistry, "\n")
cat("Number of students who failed Chemistry:", length(fail_chemistry), "\n")

# Count students who fail Physics (<=49)
fail_physics <- student_names[physics_scores <= 49]
cat("\nStudents who failed Physics (<=49):", 
    if(length(fail_physics) == 0) "None" else fail_physics, "\n")
cat("Number of students who failed Physics:", length(fail_physics), "\n")

# Find who got the highest/best score for Chemistry
best_chemistry <- max(chemistry_scores)
best_chemistry_student <- student_names[which(chemistry_scores == best_chemistry)]
cat("\nHighest Chemistry Score:", best_chemistry, "\n")
cat("Student with highest Chemistry score:", best_chemistry_student, "\n")

# Find who got the highest/best score for Physics
best_physics <- max(physics_scores)
best_physics_student <- student_names[which(physics_scores == best_physics)]
cat("\nHighest Physics Score:", best_physics, "\n")
cat("Student with highest Physics score:", best_physics_student, "\n")
