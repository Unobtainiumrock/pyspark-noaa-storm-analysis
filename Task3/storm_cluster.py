import pyspark
import math
from pyspark import SparkContext
import numpy as np
import sys
import csv

sc = pyspark.SparkContext(appName="Storm").getOrCreate()
sc.setLogLevel("ERROR")

#######################################
# John Hutchens code
#######################################
rdd = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")

header = rdd.first()
rdd1 = rdd.filter(lambda x: x != header)

rdd_split = rdd1.map(lambda x: x.split(","))

rdd_MAG = rdd_split.map(lambda x: (x[11],x[28]))

def safe_int(x):
    try:
        return int(x)
    except:
        return float('nan')

def safe_float(x):
    try:
        return float(x)
    except:
        return float('nan')
    
rdd_MAG_num = rdd_MAG.map(lambda x: (safe_int(x[0]),safe_float(x[1])))
rdd_MAG_clean = rdd_MAG_num.filter(lambda x: x[1] != 0 and x[1] != float('nan'))
rdd_MAG_clean = rdd_MAG_clean.filter(lambda x: not math.isnan(x[1]))

avg_per_year = (
    rdd_MAG_clean
    .mapValues(lambda v: (v, 1))                               
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))       
    .mapValues(lambda x: x[0] / x[1])                           
)

data_avg = list(avg_per_year.collect())

count_over_50 = (
    rdd_MAG_clean
    .filter(lambda x: x[1] > 50)
    .map(lambda x: (x[0], 1))
    .reduceByKey(lambda a, b: a + b)
)

data_over50 = list(count_over_50.collect())

count_over_20 = (
    rdd_MAG_clean
    .filter(lambda x: x[1] > 20)
    .map(lambda x: (x[0], 1))
    .reduceByKey(lambda a, b: a + b)
)

data_over20 = list(count_over_20.collect())

print(f"Mag_avg = {data_avg}")
print(f"Mag_over50 = {data_over50}")
print(f"Mag_over20 = {data_over20}")



#######################################
# Marjan Farsi Code 
#######################################
rdd = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")
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



#######################################
# Aatish code
#######################################
rdd = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")
header = rdd.first()
rdd1 = rdd.filter(lambda x: x != header)

rdd_split = rdd1.map(lambda x: x.split(","))
rdd_SET = rdd_split.map(lambda x: (x[9],x[13]))

rdd_clean = rdd_SET.filter(lambda x: x[0] not in (None, '') and x[1] not in (None, ''))
rdd_clean = rdd_clean.map(lambda x: [x, 1]).reduceByKey(lambda x,y: x+y).map(lambda x: [x[0][0], x[0][1],x[1]])
most_storm_prone_states_and_their_storms = rdd_clean.sortBy(lambda x: x[2], ascending=False)

print(most_storm_prone_states_and_their_storms.collect())

#######################################
# Alexander code
#######################################

rdd = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")

header = rdd.first()
rdd = rdd.filter(lambda x: x != header)
rdd = rdd.map(lambda x: (x.split(",")))

header=header.split(",")

directinjuries=header.index("INJURIES_DIRECT")
indirectinjuries=header.index("INJURIES_INDIRECT")
directdeaths=header.index("DEATHS_DIRECT")
indirectdeaths=header.index("DEATHS_INDIRECT")
propertydamage=header.index("DAMAGE_PROPERTY")
cropdamage=header.index("DAMAGE_CROPS")
mycolumns = [directinjuries, indirectinjuries, directdeaths, indirectdeaths, propertydamage, cropdamage]

for column in mycolumns:
    print(f"x[{column}] != '' AND")

rdd_filter = rdd.filter(lambda x: x[26] != '' and x[22] != '' and x[23] != '' and x[24] != '' and x[25] != '' and x[27] != '' and x[25] != 'K' and (x[13] == "Flood" or x[13] == "Flash Flood"))
rdd_filter = rdd_filter.map(lambda x: (x[13],  x[25]))

rdd_filter = rdd_filter.map(lambda x: (x[0],  float(x[1][:-1])*1000 if x[1][-1] == 'K' else float(x[1][:-1])*1000000 if x[1][-1] == 'M' else float(x[1][:-1])*1000000000 if x[1][-1] == 'B' else float(x[1])))
rdd_filter.collect()

from statistics import mean
rdd_property_damage = rdd_filter.reduceByKey(lambda a, b: mean([float(a), float(b)]))
property_damage=rdd_property_damage.collect()

rdd_filter = rdd.filter(lambda x: x[26] != '' and x[22] != '' and x[23] != '' and x[24] != '' and x[25] != '' and x[27] != '' and x[26] != 'K' and (x[13] == "Flood" or x[13] == "Flash Flood"))
rdd_filter = rdd_filter.map(lambda x: (x[13],  x[26]))

rdd_filter.collect()

rdd_filter = rdd_filter.filter(lambda x: x[1] != 'M')
rdd_filter.collect()

rdd_filter = rdd_filter.map(lambda x: (x[0],  float(x[1][:-1])*1000 if x[1][-1] == 'K' else float(x[1][:-1])*1000000 if x[1][-1] == 'M' else float(x[1][:-1])*1000000000 if x[1][-1] == 'B' else float(x[1])))

rdd_crop_damage = rdd_filter.reduceByKey(lambda a, b: mean([float(a), float(b)]))
crop_damage=rdd_crop_damage.collect()


rdd_filter = rdd.filter(lambda x: x[26] != '' and x[22] != '' and x[23] != '' and x[24] != '' and x[25] != '' and x[27] != '' and x[21] != 'K' and x[21] != 'M' and x[21] != 'B' and (x[13] == "Flood" or x[13] == "Flash Flood"))
rdd_filter = rdd_filter.map(lambda x: (x[13],  x[21]))


rdd_filter.collect()
rdd_filter = rdd_filter.map(lambda x: (x[0],  float(x[1][:-1])*1000 if x[1][-1] == 'K' else float(x[1][:-1])*1000000 if x[1][-1] == 'M' else float(x[1][:-1])*1000000000 if x[1][-1] == 'B' else float(x[1])))

rdd_filter.collect()



rdd_direct_injuries = rdd_filter.reduceByKey(lambda a, b: mean([float(a), float(b)]))
direct_injuries=rdd_direct_injuries.collect()

rdd_filter = rdd.filter(lambda x: x[26] != '' and x[22] != '' and x[23] != '' and x[24] != '' and x[25] != '' and x[27] != '' and x[22] != 'K' and x[22] != 'M' and x[22] != 'B' and (x[13] == "Flood" or x[13] == "Flash Flood"))
rdd_filter = rdd_filter.map(lambda x: (x[13],  x[22]))

rdd_filter = rdd_filter.map(lambda x: (x[0],  float(x[1][:-1])*1000 if x[1][-1] == 'K' else float(x[1][:-1])*1000000 if x[1][-1] == 'M' else float(x[1][:-1])*1000000000 if x[1][-1] == 'B' else float(x[1])))

rdd_indirect_injuries = rdd_filter.reduceByKey(lambda a, b: mean([float(a), float(b)]))
indirect_injuries=rdd_indirect_injuries.collect()


rdd_filter = rdd.filter(lambda x: x[26] != '' and x[22] != '' and x[23] != '' and x[24] != '' and x[25] != '' and x[27] != '' and x[23] != 'K' and x[23] != 'M' and x[23] != 'B' and (x[13] == "Flood" or x[13] == "Flash Flood"))
rdd_filter = rdd_filter.map(lambda x: (x[13],  x[23]))

rdd_filter = rdd_filter.map(lambda x: (x[0],  float(x[1][:-1])*1000 if x[1][-1] == 'K' else float(x[1][:-1])*1000000 if x[1][-1] == 'M' else float(x[1][:-1])*1000000000 if x[1][-1] == 'B' else float(x[1])))

rdd_direct_deaths = rdd_filter.reduceByKey(lambda a, b: mean([float(a), float(b)]))
direct_deaths=rdd_direct_deaths.collect()



rdd_filter = rdd.filter(lambda x: x[26] != '' and x[22] != '' and x[23] != '' and x[24] != '' and x[25] != '' and x[27] != '' and x[24] != 'K' and x[24] != 'M' and x[24] != 'B' and (x[13] == "Flood" or x[13] == "Flash Flood"))
rdd_filter = rdd_filter.map(lambda x: (x[13],  x[24]))

rdd_filter = rdd_filter.map(lambda x: (x[0],  float(x[1][:-1])*1000 if x[1][-1] == 'K' else float(x[1][:-1])*1000000 if x[1][-1] == 'M' else float(x[1][:-1])*1000000000 if x[1][-1] == 'B' else float(x[1])))
rdd_indirect_deaths2=rdd_filter.map(lambda x: x[1])

rdd_indirect_deaths = rdd_filter.reduceByKey(lambda a, b: mean([float(a), float(b)]))
indirect_deaths=rdd_indirect_deaths.collect()
print(f"Average property damage: {property_damage}")
print(f"Average crop damage: {crop_damage}")
print(f"Average direct injuries: {direct_injuries}")
print(f"Average indirect injuries: {indirect_injuries}")
print(f"Average direct deaths: {direct_deaths}")
print(f"Average indirect deaths: {indirect_deaths}")

#######################################
# Alan Luo code
#######################################
def parse_csv_alan(line):
    """Parses a CSV line safely, handling quoted fields with commas."""
    try:
        return next(csv.reader([line]))
    except:
        return []

def extract_and_clean_alan(row):
    """Extracts relevant columns and converts metrics to integers."""
    # Check length for cloud dataset (indices shifted by +1)
    if not row or len(row) < 30: 
        return None
        
    try:
        state = row[9]           
        event_type = row[13]     
        cz_name = row[16]        
        mag_type = row[29] if row[29] else "Unknown"

        def to_int(val):
            return int(float(val)) if val and val.strip() else 0

        injuries = to_int(row[21]) + to_int(row[22]) 
        deaths = to_int(row[23]) + to_int(row[24])   
        
        return (state, cz_name, event_type, mag_type, injuries, deaths)
    except:
        return None

# 1. Load Data (Re-using the hardcoded path from the group file)
rdd_alan = sc.textFile("gs://msds-694-cohort-14-group12/storm_data.csv")
header_alan = rdd_alan.first()
data_rdd_alan = rdd_alan.filter(lambda x: x != header_alan)

# 2. Parse and Clean
cleaned_rdd_alan = data_rdd_alan.map(parse_csv_alan) \
                                .map(extract_and_clean_alan) \
                                .filter(lambda x: x is not None)

# 3. Analysis
geo_impact = cleaned_rdd_alan.map(lambda x: ((x[0], x[1]), x[4] + x[5])) \
                             .reduceByKey(lambda a, b: a + b) \
                             .sortBy(lambda x: x[1], ascending=False)

print("="*50)
print("Top 10 Counties by Human Impact (Alan Luo)")
print("="*50)
for loc, count in geo_impact.take(10):
    print(f"{loc[0]} - {loc[1]}: {count}")

type_impact = cleaned_rdd_alan.map(lambda x: ((x[2], x[3]), (x[4], x[5]))) \
                              .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
                              .sortBy(lambda x: x[1][0] + x[1][1], ascending=False)

print("\n" + "="*50)
print("Top 10 Event Types by Impact (Alan Luo)")
print("="*50)
for key, stats in type_impact.take(10):
    print(f"Event: {key[0]} (Mag: {key[1]}) -> Injuries: {stats[0]}, Deaths: {stats[1]}")


#######################################
# Group member 6 code
#######################################




sc.stop()
