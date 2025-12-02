import pyspark

sc = pyspark.SparkContext(appName="PA-2").getOrCreate()
sc.setLogLevel("ERROR")


# Group member 1 code




# Group member 2 code




# Group member 3 code




# Group member 4 code




# Group member 5 code




# Group member 6 code




# stop the sc and print outputs

# filename = "/Users/mchaudhari/USF/USF-Faculty/Courses/2025/MSDS-694-Distributed-ComputingFall-2025/programming-assignments/PA-2/movies_combined.csv"
# rdd = createRDD(sc, filename)
# print(returnCount(rdd))

# print(distinctGenresByUserID(rdd, 380))

# print(getHighestRatingByMovie(rdd, "Balto (1995)"))

# print(countRatingsPerMovie(rdd))

sc.stop()
