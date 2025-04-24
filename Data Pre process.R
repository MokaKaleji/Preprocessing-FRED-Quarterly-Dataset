################################################################################
#                                Data Processing                               #
################################################################################
# Author: Moka Kaleji • Master’s Thesis, University of Bologna
Contact: mohammadkaleji1998@gmail.com                                                   
# Affiliation: Master Thesis in Econometrics             
#                                                                                 
# Description:
# This script executes the preprocessing pipeline for the FRED quarterly dataset
# (not directly accessible via source). It performs data cleaning, outlier detection,
# transformation to stationarity, and imputation, preparing the series for factor
# modeling and further econometric analysis.
#                                                                                 
# Usage:
#   Call Preprocessing(QDLT, C) with:
#     C = 1 : Retain more variables (fewer observations)
#     C = 2 : Retain more observations (fewer variables)
################################################################################

########################## Load Required Libraries #############################
library(zoo)       # Provides indexed time series objects for temporal operations
library(readxl)    # Enables reading data from Excel workbooks
library(dplyr)     # Grammar of data manipulation for pipeline transformations
library(imputeTS)  # Offers methods for imputing missing data in time series
library(reshape2)  # Tools for reshaping wide and long data frames
library(writexl)   # Export data frames to Excel files
################################################################################

################################ Data Import ###################################
# Define file path for raw data
file_path <- "/Users/moka/Research/Thesis/Live Project/Raw_Data/QDLT.xlsx"

# Read the full dataset from the Excel file into R
QDLT <- read_excel(file_path)
################################################################################

########################## Preprocessing Function ##############################
Preprocessing <- function(QDLT, C = NULL) {
  # This function branches into two modes based on C:
  # 1) C == 1: Prioritize variable count (fewer time points)
  # 2) C == 2: Prioritize time span (fewer variables retained)
  
  if (C == 1) {
    # --- Subset to relevant date range for high-dimensional analysis ---
    QD_Cleaned <- QDLT[82:265, ]            # Rows corresponding to target period
    QD_Cleaned <- rbind(QDLT[1, ], QD_Cleaned)  # Prepend header row of metadata
    rownames(QD_Cleaned) <- NULL             # Reset row names for consistency
    
    # --- Outlier detection via IQR rule ---
    remove_outliers_IQR <- function(X, c = 10) {
      # Convert data frame to matrix if necessary for numeric operations
      if (is.data.frame(X)) X <- as.matrix(X)
      # Compute column-wise median and interquartile range (IQR)
      medians <- apply(X, 2, median, na.rm = TRUE)
      iqr_vals <- apply(X, 2, IQR, na.rm = TRUE)
      # Expand these statistics to full matrix dimension
      med_mat <- matrix(medians, nrow = nrow(X), ncol = length(medians), byrow = TRUE)
      iqr_mat <- matrix(iqr_vals, nrow = nrow(X), ncol = length(iqr_vals), byrow = TRUE)
      # Flag observations exceeding c * IQR from median as outliers
      outlier_flag <- abs(X - med_mat) > c * iqr_mat
      # Replace outliers with NA for subsequent imputation
      X[outlier_flag] <- NA
      # Return cleaned data matrix, logical outlier map, and counts per column
      return(list(X = X, out = outlier_flag, n = colSums(outlier_flag, na.rm = TRUE)))
    }
    
    # Extract numeric block excluding date column and first row metadata
    x <- QD_Cleaned[2:185, ]
    QD_IQR <- remove_outliers_IQR(x[, -1])
    QD_IQR_df <- as.data.frame(QD_IQR$X)
    QD_IQR_df <- rbind(QD_Cleaned[1, 2:246], QD_IQR_df)  # Reattach metadata row
    
    # --- Transformation to stationarity ---
    transformation_codes <- as.numeric(QD_IQR_df[1, ])
    data_values <- as.data.frame(lapply(QD_IQR_df[-1, ], as.numeric))
    transformed_list <- vector("list", length(transformation_codes))
    
    for (i in seq_along(transformation_codes)) {
      code <- transformation_codes[i]
      series <- data_values[[i]]
      # Branch based on FRED-QD transformation code:
      if (code == 1) {
        # Level series preserved
        transformed <- series
      } else if (code == 5) {
        # Log-difference: 100 * delta log(series)
        transformed <- c(NA, diff(100 * log(series)))
      } else if (code == 2) {
        # First-difference: delta(series)
        transformed <- c(NA, diff(series))
      } else if (code == 7) {
        # Double-difference on growth rates: delta(ratio)
        ratio <- series[-1] / series[-length(series)] - 1
        transformed <- c(NA, NA, diff(ratio))
      } else {
        stop(paste("Unknown transformation code in column", i))
      }
      transformed_list[[i]] <- transformed
    }
    
    # Combine transformed vectors into data frame
    QD_Stationary <- as.data.frame(transformed_list)
    names(QD_Stationary) <- names(QD_IQR_df)
    
    # --- Handle missing data ---
    missing_prop <- colMeans(is.na(QD_Stationary))  # Proportion of NAs by column
    removed_columns <- names(missing_prop[missing_prop > 0.50])
    print(removed_columns)                            # Report dropped variables
    QD_Stationary <- QD_Stationary[, missing_prop <= 0.50]
    
    # --- Imputation via Kalman filter (EM algorithm) ---
    QD_Final <- na_kalman(QD_Stationary)
    # Reattach date index from cleaned data
    QD_Final <- cbind(QD_Cleaned[2:185, 1], QD_Final)
    
    # --- Export process output ---
    output_file_path <- "/Users/moka/Research/Thesis/Live Project/Processed_Data/QD1979.xlsx"
    write_xlsx(QD_Final, path = output_file_path)
    message("✅ Final dataset saved as 'QD1979.xlsx'")
    
  } else if (C == 2) {
    # --- Mode 2: Retain longest time span, drop sparse variables ---
    missing_prop <- colMeans(is.na(QDLT))
    removed_columns <- names(missing_prop[missing_prop > 0.70])
    print(removed_columns)
    QD_Cleaned <- QDLT[, missing_prop <= 0.70]
    
    x <- QD_Cleaned[2:265, ]
    # Count initial consecutive NAs to remove series with long NA head
    count_na_start <- function(x) sum(cumprod(is.na(x)))
    na_start_counts <- sapply(x, count_na_start)
    removing_vars <- names(na_start_counts[na_start_counts >= 64])
    QD_Cleaned <- QD_Cleaned[, !names(QD_Cleaned) %in% removing_vars]
    print(removing_vars)
    
    # Repeat outlier removal, stationarity transform, and imputation as above
    remove_outliers_IQR <- function(X, c = 10) {
      if (is.data.frame(X)) X <- as.matrix(X)
      medians <- apply(X, 2, median, na.rm = TRUE)
      iqr_vals <- apply(X, 2, IQR, na.rm = TRUE)
      med_mat <- matrix(medians, nrow = nrow(X), ncol = length(medians), byrow = TRUE)
      iqr_mat <- matrix(iqr_vals, nrow = nrow(X), ncol = length(iqr_vals), byrow = TRUE)
      outlier_flag <- abs(X - med_mat) > c * iqr_mat
      X[outlier_flag] <- NA
      return(list(X = X, out = outlier_flag, n = colSums(outlier_flag, na.rm = TRUE)))
    }
    
    QD_IQR <- remove_outliers_IQR(x[, -1])
    QD_IQR_df <- as.data.frame(QD_IQR$X)
    QD_IQR_df <- rbind(QD_Cleaned[1, 2:ncol(QD_IQR_df)+1], QD_IQR_df)
    
    transformation_codes <- as.numeric(QD_IQR_df[1, ])
    data_values <- as.data.frame(lapply(QD_IQR_df[-1, ], as.numeric))
    transformed_list <- vector("list", length(transformation_codes))
    
    for (i in seq_along(transformation_codes)) {
      code <- transformation_codes[i]; series <- data_values[[i]]
      if (code == 1) {
        transformed <- series
      } else if (code == 5) {
        transformed <- c(NA, diff(100 * log(series)))
      } else if (code == 2) {
        transformed <- c(NA, diff(series))
      } else if (code == 7) {
        ratio <- series[-1] / series[-length(series)] - 1
        transformed <- c(NA, NA, diff(ratio))
      } else {
        stop(paste("Unknown transformation code in column", i))
      }
      transformed_list[[i]] <- transformed
    }
    
    QD_Stationary <- as.data.frame(transformed_list)
    names(QD_Stationary) <- names(QD_IQR_df)
    
    missing_prop <- colMeans(is.na(QD_Stationary))
    removed_columns <- names(missing_prop[missing_prop > 0.50])
    print(removed_columns)
    QD_Stationary <- QD_Stationary[, missing_prop <= 0.50]
    
    QD_Final <- na_kalman(QD_Stationary)
    QD_Final <- cbind(QD_Cleaned[2:265, 1], QD_Final)
    
    output_file_path <- "/Users/moka/Research/Thesis/Live Project/Processed_Data/QD1959.xlsx"
    write_xlsx(QD_Final, path = output_file_path)
    message("✅ Final dataset saved as 'QD1959.xlsx'")
  }
}

# Execute both modes:
Preprocessing(QDLT, C = 1)  # Mode 1: More variables, shorter span
Preprocessing(QDLT, C = 2)  # Mode 2: More observations, fewer variables
