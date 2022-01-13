'''
Optimize the query plan
Suppose we want to compose query in which we get for each question also the number of answers to this question for each month. 
See the query below which does that in a suboptimal way and try to rewrite it to achieve a more optimal plan.
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month
import os
from pyspark.sql.types import DateType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

spark = SparkSession.builder.master('local').appName('Optimize I').getOrCreate()
base_path = os.getcwd()
project_path = ('/').join(base_path.split('/')[0:-3]) 
answers_input_path = os.path.join(project_path, 'Optimization/data/answers')
questions_input_path = os.path.join(project_path, 'Optimization/output/questions-transformed')

answersSchema = StructType([
    StructField("question_id", LongType(), True),
    StructField("creation_date", TimestampType(), True)])

questionsSchema = StructType([
    StructField("question_id", LongType(), True),
    StructField("creation_date", TimestampType(), True),
    StructField("title", StringType(), True)])

answersDF = spark.read.schema(answersSchema).parquet(answers_input_path).select('question_id','creation_date')
questionsDF = spark.read.schema(questionsSchema).parquet(questions_input_path).select('question_id','creation_date','title')

'''
Answers aggregation
Here we : get number of answers per question per month
'''

joinedDF = questionsDF.join(answersDF, 'question_id').select(questionsDF['question_id'], questionsDF['creation_date'], questionsDF['title'], answersDF['creation_date'].alias('ans_date'))
resultDF = joinedDF.withColumn('month', month('ans_date')).groupBy('question_id', 'creation_date', 'title', 'month').agg(count('*').alias('cnt')).select('*')
# resultDF.explain(mode='formatted')
resultDF.orderBy('question_id', 'month').show()

