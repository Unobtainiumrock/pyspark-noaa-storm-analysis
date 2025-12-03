import pyspark
import math

sc = pyspark.SparkContext(appName="Storm").getOrCreate()
sc.setLogLevel("ERROR")


# John Hutchens code
rdd = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")

header = rdd.first()
rdd1 = rdd.filter(lambda x: x != header)

rdd_split = rdd1.map(lambda x: x.split(","))

rdd_MAG = rdd_split.map(lambda x: (x[10],x[27]))

def safe_float(x):
    try:
        return float(x)
    except:
        return float('nan')
    
rdd_MAG_num = rdd_MAG.map(lambda x: (int(x[0]),safe_float(x[1])))
rdd_MAG_clean = rdd_MAG_num.filter(lambda x: x[1] != 0 and x[1] != float('nan'))
rdd_MAG_clean = rdd_MAG_clean.filter(lambda x: not math.isnan(x[1]))

avg_per_year = (
    rdd_MAG_clean
    .mapValues(lambda v: (v, 1))                               
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))       
    .mapValues(lambda x: x[0] / x[1])                           
)

data_avg = avg_per_year.collect()

count_over_50 = (
    rdd_MAG_clean
    .filter(lambda x: x[1] > 50)
    .map(lambda x: (x[0], 1))
    .reduceByKey(lambda a, b: a + b)
)

data_over50 = count_over_50.collect()

count_over_20 = (
    rdd_MAG_clean
    .filter(lambda x: x[1] > 20)
    .map(lambda x: (x[0], 1))
    .reduceByKey(lambda a, b: a + b)
)

data_over20 = count_over_20.collect()

print(f"Mag_avg = {data_avg}")
print(f"Mag_over50 = {data_over50}")
print(f"Mag_over20 = {data_over20}")




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
