# Spark Course

[01 Ratings Histogram](https://github.com/dongzhang84/PySpark/blob/master/Spark_Course/01_Ratings_histogram.ipynb):

collections.OrderedDict

[02 Friends by Age](https://github.com/dongzhang84/PySpark/blob/master/Spark_Course/02_Friends_by_Age.ipynb):

dictionary (key, value), sum value: 
rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
