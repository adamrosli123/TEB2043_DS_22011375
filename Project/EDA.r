# Load the FIFA player dataset into a data frame
df <- read.csv("fifa_player_performance_market_value.csv")

# Standardize column names: convert to lowercase and replace special characters 
# with underscores to prevent syntax errors during analysis
names(df) <- tolower(gsub("[^a-zA-Z0-9]", "_", names(df)))

cat("===== DATASET STRUCTURE =====\n")
# Inspect the internal structure of the data frame (data types and first few entries)
str(df)

cat("\n===== SUMMARY STATISTICS =====\n")
# Generate descriptive statistics (min, max, median, mean) for all columns
summary(df)

# Print the total dimensions of the dataset
cat("\nRows:", nrow(df), "| Columns:", ncol(df), "\n")

cat("\n===== MISSING VALUES =====\n")
# Check for null or missing values across all columns
missing <- colSums(is.na(df))
if (any(missing > 0)) {
  # If missing values exist, print the affected columns and their counts
  print(missing[missing > 0])
} else {
  cat("No missing values found in the dataset.\n")
}

# ==========================================
# Visualizations
# ==========================================

# Configure plotting layout: set a 2x2 grid to display 4 charts together.
# Increased the bottom margin ('mar') so rotated text labels are not cut off.
par(mfrow = c(2, 2), mar = c(6, 4, 3, 2))

# 1. Market Value by Position (Boxplot)
# Purpose: Analyzes the financial distribution and outliers across different playing positions.
boxplot(market_value_million_eur ~ position, data = df,
        main = "Market Value Spread by Position",
        ylab = "Market Value (M EUR)", xlab = "",
        col = "#81D4FA", border = "#0277BD", 
        las = 2) # las = 2 rotates the x-axis labels vertically

# 2. Age vs Potential Rating (Scatterplot)
# Purpose: Explores the relationship between player age and their perceived future growth.
plot(df$age, df$potential_rating,
     main = "Age vs Potential Rating",
     xlab = "Age", ylab = "Potential Rating",
     col = adjustcolor("#FF9800", alpha.f = 0.4), 
     pch = 16)

# 3. Players Distribution by Club (Bar Chart)
# Purpose: Displays the representation and squad size of each club in the dataset.
club_counts <- sort(table(df$club), decreasing = TRUE)
barplot(club_counts,
        main = "Number of Players by Club",
        ylab = "Count",
        col = "#A5D6A7", border = "#2E7D32",
        las = 2, cex.names = 0.8)

# 4. Matches Played vs Goals (Scatterplot)
# Purpose: Examines scoring efficiency by comparing goals scored against game appearances.
plot(df$matches_played, df$goals,
     main = "Goals vs Matches Played",
     xlab = "Matches Played", ylab = "Goals Scored",
     col = adjustcolor("#E91E63", alpha.f = 0.4), 
     pch = 16)

# Reset graphical parameters back to system defaults (1x1 layout, standard margins)
par(mfrow = c(1, 1), mar = c(5, 4, 4, 2) + 0.1)

# ==========================================
# Data Insights
# ==========================================

cat("\n===== TOP 5 MOST VALUABLE PLAYERS =====\n")
# Sort the data frame in descending order based on market value
top5 <- df[order(-df$market_value_million_eur), ]

# Extract and display the key attributes of the top 5 most expensive players
print(top5[1:5, c("player_name", "age", "club", "position",
                  "overall_rating", "market_value_million_eur")], row.names = FALSE)
