# DIC-prediction-2025

This repository contains code for extracting data from the OneICU database and developing predictive models for disseminated intravascular coagulation (DIC) in patients with sepsis. The project aims to build an interpretable and reproducible pipeline for DIC prediction using real-world clinical data.

---

## Table of Contents

- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Requirements](#requirements)
- [Usage](#usage)
  - [SQL Scripts](#sql-scripts)
  - [Python Scripts](#python-scripts)
- [License](#license)
- [Contact](#contact)

---

## Overview

The **DIC-prediction-2025** repository includes:

1. **SQL scripts** to extract and preprocess patient data related to DIC from the OneICU database.
2. **Python code** to train machine learning models and evaluate performance using metrics such as AUROC and SHAP values.

The workflow enables researchers to reproduce the steps of data preparation, model training, and interpretation in a modular and transparent manner.

---

## Repository Structure

```
DIC-prediction-2025
├── README.md
├── LICENSE
├── sql/
│   └── ...
└── python/
    └── ...
```

- **sql/**  
  - Contains SQL scripts for selecting relevant features, outcomes, and cohorts from the OneICU clinical database.

- **python/**  
  - Contains Python scripts for:
    - Data preprocessing
    - Model training using AutoML
    - Model evaluation (e.g., ROC curves)
    - Model explanation (e.g., SHAP plots)

---

## Requirements
1. **Google BigQuery Access**
  - To run the SQL scripts, you will need access to Google BigQuery and appropriate credentials to query OneICU database.

2. **Python**
  - You will need Python installed (version 3.10 or higher is recommended) to run the Python scripts and generate figures.

3. **Python Packages**  
  - Common data science packages such as `pandas`, `numpy`, `matplotlib`, and `seaborn`.  
  - Machine learning tools including `scikit-learn` and `h2o`.  
  - Please check the top of each Python script for specific library requirements.

---
## Usage

### SQL Scripts
1. **Navigate** to the sql directory.
2. **Open** the SQL script of interest.  
3. **Copy** the script into your BigQuery console.
4. **Run** the query.  
   - Ensure you have access to the respective dataset(s) and that your [BigQuery billing project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) is configured correctly.

### Python Scripts
1. **Clone** this repository or download the files locally.
2. **Open** your Python environment.
3. **Install** any missing Python packages.
4. **Run** the scripts in the `python` folder in the recommended order to:
   - Perform data preprocessing
   - Train predictive models using H2O AutoML
   - Evaluate performance using AUROC and other metrics
   - Generate model interpretation outputs (e.g., SHAP values)

---

## Contact

For questions or collaboration inquiries, please reach out to us by email:

- [MeDiCU, Inc.](mailto:info@medicu.co.jp)

---

## License
This project is licensed under the GNU General Public License (GPL) - see the [LICENSE.md](LICENSE.md) file for details.

---

**Disclaimer:**  
The code in this repository is provided for academic research and educational purposes. Individual patient data are not provided.
