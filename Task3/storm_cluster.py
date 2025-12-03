import pyspark
import math
from pyspark import SparkContext
import numpy as np

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




# Marjan Farsi Code 

sc = SparkContext("local", "Storm_Tornado_Analysis")

rdd = sc.textFile("storm_g2020.csv")

header = rdd.first()
columns = header.split(",")

rdd = rdd.filter(lambda row: row != header)
split_rdd = rdd.map(lambda line: line.split(","))


# Get columns indexes

event_type_idx = columns.index("EVENT_TYPE")
state_idx = columns.index("STATE")

damage_prop_idx = columns.index("DAMAGE_PROPERTY")
damage_crop_idx = columns.index("DAMAGE_CROPS")

deaths_direct_idx = columns.index("DEATHS_DIRECT")
deaths_indirect_idx = columns.index("DEATHS_INDIRECT")
inj_direct_idx = columns.index("INJURIES_DIRECT")
inj_indirect_idx = columns.index("INJURIES_INDIRECT")


# Clean missing data and convert data to appropriate datatypes

def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0

def safe_int(x):
    try:
        return int(x)
    except:
        return 0

def fill_and_cast(record):
    # Replace missing state or event type
    record[event_type_idx] = record[event_type_idx] or "Unknown"
    record[state_idx] = record[state_idx] or "Unknown"
    return record

clean_rdd = split_rdd.map(fill_and_cast)


# Filter only Tornado events

tornado_rdd = clean_rdd.filter(lambda x: x[event_type_idx] == "Tornado")


# Count total Tornados by states

tornado_count = (
    tornado_rdd
    .map(lambda x: (x[state_idx], 1))
    .reduceByKey(lambda a, b: a + b)
)


# Combine different severity indicators (Define severe tornado rule)

'''
Indicators of severity, including:

    - Property damage
    - Crop damage
    - Direct & indirect deaths
    - Direct & indirect injuries
'''
def is_severe(record):
    prop = safe_float(record[damage_prop_idx])
    crop = safe_float(record[damage_crop_idx])
    deaths = safe_int(record[deaths_direct_idx]) + safe_int(record[deaths_indirect_idx])
    injuries = safe_int(record[inj_direct_idx]) + safe_int(record[inj_indirect_idx])

    return (prop > 50000) or (crop > 50000) or (deaths > 0) or (injuries > 5)


# Filter severe tornados based on the above rule

severe_rdd = tornado_rdd.filter(is_severe)


# Count severe tornados by State

severe_count = (
    severe_rdd
    .map(lambda x: (x[state_idx], 1))
    .reduceByKey(lambda a, b: a + b)
)

# Collect results
tornado_data = tornado_count.collect()
severe_data = severe_count.collect()

# Sort for plotting
tornado_data = sorted(tornado_data, key=lambda x: -x[1])
severe_data = sorted(severe_data, key=lambda x: -x[1])

print("\n--- Number of SEVERE TORNADOES Per State ---")
for state, count in severe_data:
    print(f"{state}: {count}")

# Extract for plotting

states = [x[0] for x in tornado_data]
t_counts = [x[1] for x in tornado_data]

severe_dict = dict(severe_data)
s_counts = [severe_dict.get(state, 0) for state in states]




# Aatish code

rdd = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")
header = rdd.first()
rdd1 = rdd.filter(lambda x: x != header)

rdd_split = rdd1.map(lambda x: x.split(","))
rdd_SET = rdd_split.map(lambda x: (x[8],x[12]))

rdd_clean = rdd_SET.filter(lambda x: x[0] not in (None, '') and x[1] not in (None, ''))
rdd_clean = rdd_clean.map(lambda x: [x, 1]).reduceByKey(lambda x,y: x+y).map(lambda x: [x[0][0], x[0][1],x[1]])
most_storm_prone_states_and_their_storms = rdd_clean.sortBy(lambda x: x[2], ascending=False)


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
