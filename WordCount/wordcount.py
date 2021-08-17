'''
@Author: Ayur Ninawe
@Date: 2021-08-17
@Last Modified by: Ayur Ninawe
@Last Modified time: 18:00:47 2021-08-17
@Title : write a program for wordcount in pyspark.
'''

from pyspark import SparkContext
from logger import logger

def word_count():
    """
    Description:
        This function will count words that are present in file provided in path and willstore output.
    """
    try:
        sc = SparkContext("local","word count program")
        words = sc.textFile("hdfs://localhost:9000/sparkData/words.txt").flatMap(lambda x:x.split(" "))
        wordcounts = words.map(lambda word: (word,1)).reduceByKey(lambda a,b:a+b)
        wordcounts.saveAsTextFile("hdfs://localhost:9000/sparkData/wordcountOutPut")
        logger.info("Output stored Successfully")
    except Exception as e:
        logger.info(f"Erorr!!{e}")

word_count()