# Spark Optimization Mini-Project

## **Overview**:
### Data for the mini-project: 
####	4 "questions" parquet files (\Optimization\output\questions-transformed)
####	4 "answers" parquet files (\Optimization\data\answers)
### The task is to rewrite and optimze the \Optimization\**optimize.py** script
### The optimized version is here: \Optimization\**optimized.py**
### Steps taken to optimze the script include:
#### 1. Use spark.read.parquet with Schema predefined (answersSchema / questionsSchema), instead of spark.read.option('path',..)
#### 2. Only selected the 5 columns that are actually used initially from both files
#### 3. Performed the JOIN to create joinedDF (which should filter the results) before applying the groupBy.agg operation






