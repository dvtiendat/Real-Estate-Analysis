## Real-Estate-Analysis

A capstone project focusing on estate analysis and prediction 

## Features

This repo contains a full pipeline from data collection to transforming \& integrating data and pushing to the final database (located in pipeline.py)

The real-estate data are crawled from three websites: "Alonhadat.com.vn", "Batdongsan.so", and "Batdongsan.com.vn".

Notebooks for exploratory data analysis are located in the EDA folder (EDA/EDA.ipynb)

Other notebooks for modeling (XGBoost, Ridge Regression, Random Forest) (Modeling/XGBoost.ipynb, Modeling/RidgeRegression.ipynb, RandomForest.ipynb)

## Requirement

To run the pipeline for the data collection, you must have installed Python version >= 3.9. It is also highly recommended that you disable the Path Length Limit when installing Python, which will avoid any issues when installing the necessary libraries to run the project.

## How to use: 
To use this project using git, please do the following.
* First, open the command prompt / or any related terminals in the directory of the project

* Secondly, install required libraries via requirement.txt

```pip install -q -r requirements.txt```
* Finally, run the pipeline of the project by

```python pipeline.py```
