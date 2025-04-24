# Preprocessing-FRED-Quarterly-Dataset-

**Author:** Moka Kaleji • Contact: mohammadkaleji1998@gmail.com 

**Affiliation:** Master’s Thesis, University of Bologna  

This repository contains the R script for cleaning and transforming the FRED-QD macroeconomic dataset as part of my Master’s thesis. It standardizes, outlier-filters, and imputes missing values to prepare the data for factor model estimation.

### Files
- **preprocessing.R**  
  - Cleans raw Excel input (`QDLT.xlsx`)
  - Detects outliers via IQR
  - Applies Lite transformation codes (just differences, and log-differences so remember to changes the code)
  - Imputes with a Kalman filter (EM algorithm)
  - Exports two processed outputs:
    - **`QD1979.xlsx`** (retain more variables)
    - **`QD1959.xlsx`** (retain more observations)

### Usage

1. Install R packages:
   ```bash
   install.packages(c("zoo","readxl","dplyr","imputeTS","reshape2","writexl"))

2. Place your raw file at:
Raw_Data/QDLT.xlsx

3. Run in R:
source("preprocessing.R")
QDLT <- read_excel("Raw_Data/QDLT.xlsx")
Preprocessing(QDLT, C = 1)  # More variables
Preprocessing(QDLT, C = 2)  # More observations




## See Also
- [Locally-Stationary-Factor-Model](https://github.com/moka-kaleji/Locally-Stationary-Factor-Model)
- [Bayesian-Optimized-Locally-Stationary-Factor-Model](https://github.com/moka-kaleji/Bayesian-Optimized-Locally-Stationary-Factor-Model)
- [Dynamic-Factor-Model](https://github.com/moka-kaleji/Dynamic-Factor-Model)
