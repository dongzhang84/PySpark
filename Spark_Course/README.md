# Spark Course

[01 Ratings Histogram](https://github.com/dongzhang84/PySpark/blob/master/Spark_Course/01_Ratings_histogram.ipynb):

collections.OrderedDict

[02 Friends by Age](https://github.com/dongzhang84/PySpark/blob/master/Spark_Course/02_Friends_by_Age.ipynb):

dictionary (key, value), sum value: 

rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

[03 Minum Temperature](https://github.com/dongzhang84/PySpark/blob/master/Spark_Course/03_minum_temperature.ipynb):

Filter: 

parsedLines.filter(lambda x: "TMIN" in x[1])

select min:

stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
results = minTemps.collect()

[04](Word Count)(https://github.com/dongzhang84/PySpark/blob/master/Spark_Course/04_word_count.ipynb)

Split Word:

input = sc.textFile("/content/gdrive/My Drive/Spark_course/data/book.txt")
words = input.flatMap(lambda x: x.split())

Count by Value:

wordCounts = words.countByValue()
