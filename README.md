# Tennis Match Prediction System

A comprehensive system for analyzing tennis matches, building predictive models, and running betting simulations.

## Core Components

### 1. Data Processing & Feature Engineering (`data/notebooks/base-table.ipynb`)
- Creates a base table combining match data from multiple sources
- Calculates ELO ratings for players
- Processes match statistics and point-by-point data
- Handles tournament categorization and round classification
- Stores processed data in DuckDB for efficient querying

### 2. Model Training (`model/notebooks/train-model.ipynb`)
- Trains machine learning models (XGBoost, LightGBM, CatBoost) for match prediction
- Features include:
  - Player statistics (serves, returns, winners, errors)
  - Tournament information
  - Historical performance metrics
  - ELO ratings
  - Point-by-point derived features
- Uses Optuna for hyperparameter optimization
- Includes cross-validation across different time periods
- Evaluates models using metrics like Brier score and ROI

### 3. Simulation Engine (`simulation/run-simulation.py`)
- Simulates betting strategies using historical Betfair market data
- Features:
  - Multi-processing capabilities for efficient simulation
  - Customizable stake sizes and entry conditions
  - Market timing controls (e.g., time-to-jump restrictions)
  - Detailed logging of orders and performance
  - Integration with Flumine simulation framework

## Key Features
- Support for multiple tennis tournament categories (ATP, Challenger, ITF)
- Comprehensive feature engineering including:
  - ELO ratings
  - Surface-specific performance metrics
  - Tournament-specific statistics
  - Point-by-point derived probabilities
- Backtesting capabilities with realistic market simulation
- Performance analysis and optimization tools

## Requirements
- Python 3.x
- Key libraries: pandas, duckdb, xgboost, lightgbm, catboost, flumine
- Access to historical tennis match and betting market data

## Usage
1. Run data processing notebook to create base feature set
2. Train and optimize models using the training notebook
3. Run simulations to test betting strategies with trained models