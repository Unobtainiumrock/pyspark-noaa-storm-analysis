# Nicholas Task 2 Plan: Predictors of Human Harm

## Your Research Question
**Which factors are the strongest predictors of human harm? Can you engineer a new feature (e.g., 'population density of CZ_NAME') and use a model like a Random Forest to determine if 'human' factors like location are more predictive of injuries/deaths than 'storm' factors like EVENT_TYPE or MAGNITUDE_TYPE?**

## Task 2 Scope (Summarization/Aggregation)
**For Task 2, focus on data preparation and preliminary analysis. The full Random Forest model will come in later phases.**

### What You CAN Do Now (Task 2):
1. **Aggregate Human Harm Metrics**
   - Sum INJURIES_DIRECT, INJURIES_INDIRECT, DEATHS_DIRECT, DEATHS_INDIRECT
   - Group by: STATE, CZ_NAME, EVENT_TYPE, MAGNITUDE_TYPE
   - Create total harm metric: `TOTAL_HARM = INJURIES_DIRECT + INJURIES_INDIRECT + DEATHS_DIRECT + DEATHS_INDIRECT`

2. **Summary Statistics by Factor Type**
   - **Storm Factors**: Aggregate by EVENT_TYPE and MAGNITUDE_TYPE
   - **Location Factors**: Aggregate by STATE and CZ_NAME
   - Compare average harm rates between these groups

3. **Data Preparation for Modeling**
   - Identify which CZ_NAME values have the most events
   - Calculate event frequency per location (proxy for population density)
   - Create preliminary feature: `EVENT_COUNT_PER_CZ` (more events = likely more populated)

4. **Preliminary Insights**
   - Which EVENT_TYPEs cause most harm?
   - Which MAGNITUDE_TYPEs correlate with higher harm?
   - Which locations (STATE/CZ_NAME) see most harm?
   - Basic correlation analysis

### What to SAVE for Later Phases:
- Full Random Forest model training
- Population density feature engineering (need external data source)
- Feature importance calculations (Permutation, Gini, SHAP)
- Interaction features (e.g., `population_density * magnitude`)
- Model evaluation and interpretation

## Column Mapping (from dataset)
Based on the 51 columns, here are key indices:
- Column 8: STATE
- Column 16: CZ_NAME (County/Zone name)
- Column 13: EVENT_TYPE
- Column 29: MAGNITUDE_TYPE
- Column 28: MAGNITUDE
- Column 22: INJURIES_DIRECT
- Column 23: INJURIES_INDIRECT
- Column 24: DEATHS_DIRECT
- Column 25: DEATHS_INDIRECT

## Recommended Spark Aggregations for Task 2

### 1. Human Harm by Storm Factors
```python
# Aggregate by EVENT_TYPE
harm_by_event_type = rdd.map(...).reduceByKey(...)

# Aggregate by MAGNITUDE_TYPE  
harm_by_magnitude_type = rdd.map(...).reduceByKey(...)
```

### 2. Human Harm by Location Factors
```python
# Aggregate by STATE
harm_by_state = rdd.map(...).reduceByKey(...)

# Aggregate by CZ_NAME
harm_by_cz = rdd.map(...).reduceByKey(...)
```

### 3. Combined Analysis
```python
# Harm by (EVENT_TYPE, STATE) pairs
harm_by_event_state = rdd.map(...).reduceByKey(...)

# Event frequency per location (proxy for population)
event_count_by_cz = rdd.map(...).countByKey()
```

## Deliverable for Task 2
- PySpark code showing aggregations
- Summary statistics comparing storm vs. location factors
- Screenshot of results
- Brief comment explaining how this prepares for Random Forest modeling

