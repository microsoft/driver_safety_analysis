# Databricks notebook source
## Verify we have the latest packages
## numpy 1.14.0 is required here. there's a bug with fitting an distribution with older versions of numpy.

import numpy
numpy.__version__

# COMMAND ----------

import pandas as pd
import numpy as np
dfRaw = spark.createDataFrame(pd.read_csv('https://raw.githubusercontent.com/omri374/driver_behavior_analysis/master/dataset.csv'))

# COMMAND ----------

display(dfRaw)

# COMMAND ----------

import pyspark.sql.functions as psf

RELEVANT_EVENTS = ['Harsh Acceleration', 'Reached max speed', 'Out of max speed',
       'Harsh Braking', 'Harsh Turn (motion based)',
       'Harsh Braking (motion based)', 'Harsh Acceleration (motion based)',
       'Harsh Turn Left (motion based)', 'Harsh Turn Right (motion based)']

## Filter out unwanted events
df = dfRaw.filter(dfRaw.EventName.isin(RELEVANT_EVENTS))
print ("{} samples after removing events that are not relevant for modeling".format(df.count()))
df = df.select("DriverId","ts","EventName","Latitude","Longitude","Speed km/h")


####### RUN PER POPULATION SEGMENT #######

## Keep only the drivers in the current segment. This notebook runs multiple times in DataBricks, once for each population segment.
## Each time we filter the drivers in this segment and perform the analysis on these drivers. 
## It is commented out since we don't have any segments in the sample data.

#populationSegment = Segments.filter('SegmentId == "' + segmentId + '"')
#df = df.join(PopulationSegment,"DriverId")
#print ('number of drivers: {}'.format(populationSegment.count()))
#print(str(df.count()) + " Events after segmentation")

##########################################

# Remove NAs
df = df.dropna()
print("Removed NAs")

# Filter out users with too few samples
minRecordsPerDriver=50

subs = df.groupBy('DriverId').count().filter('count>{}'.format(minRecordsPerDriver))
print('Number of drivers = {}'.format(subs.count()))
df = df.join(subs,['DriverId'])

cnt = df.count()
print("{} events remained after cleansing".format(cnt))

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType
import pandas as pd
import numpy as np
from pyspark.sql.types import *

schema = StructType([
  StructField("Distance", FloatType()),
  StructField("DriverId", IntegerType())

])


def haversine(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2

    c = 2 * np.arcsin(np.sqrt(a))
    km = 6367 * c
    return km

@pandas_udf(schema,PandasUDFType.GROUPED_MAP)
def total_distance(oneDriver):
    dist = haversine(oneDriver.Longitude.shift(1), oneDriver.Latitude.shift(1),
                 oneDriver.loc[1:, 'Longitude'], oneDriver.loc[1:, 'Latitude'])
    return pd.DataFrame({"DriverId":oneDriver['DriverId'].iloc[0],"Distance":np.sum(dist)},index = [0])

## Calculate the overall distance made by each subscriber
distancePerDriver = dfRaw.groupBy('DriverId').apply(total_distance)
#distancePerDriver = distancePerDriver[distancePerDriver.Distance > 0]
distancePerDriver.sort('DriverId').show()

# COMMAND ----------

featureNames = ['Harsh Turn (motion based)','Harsh Acceleration (motion based)','Harsh Braking (motion based)','OverSpeed']
colNames = featureNames[:]
colNames.append('DriverId')

print("Starting feature engineering with {} samples".format(df.count()))

## Pivot the data from events to features: 1. Count number of events per type per driver, 2. Pivot into a data frames of drivers X features
subs = df.groupby('DriverId').count()

eventsPerDriver = df.groupby(["DriverId","EventName"]).count()

eventsPerDriver = eventsPerDriver.groupby("DriverId").pivot('EventName').sum('count')
eventsPerDriver = eventsPerDriver.na.fill(0)


## Add total drive duration per driver
eventsPerDriver = distancePerDriver.join(eventsPerDriver,'DriverId')
print("{} drivers after join with distances".format(eventsPerDriver.count()))
## Divide the number of events per subscriber with the total driver duration per subscriber
for fea in featureNames:
    if fea in eventsPerDriver.columns:
        eventsPerDriver = eventsPerDriver.withColumn(fea,psf.col(fea)/eventsPerDriver.Distance)
    else:
        eventsPerDriver = eventsPerDriver.withColumn(fea,psf.lit(0))

## Keep only feature columns

# COMMAND ----------

features = eventsPerDriver.toPandas().set_index('DriverId')

# COMMAND ----------



# COMMAND ----------

import scipy.stats as st

def transform_to_normal(x):
    xt = np.zeros(len(x))
    if np.count_nonzero(x) == 0:
        print("only zero valued values found")
        return x
    
    valueGreaterThanZero = np.where(x<=0,0,1)
    positives = x[valueGreaterThanZero == 1]
    if(len(positives)> 0):
        xt[valueGreaterThanZero == 1],_ = st.boxcox(positives+1)
    xt = xt - np.min(xt)
    return xt

def replace_outliers_with_limit(x, stdFactor = 2.5):
    x = x.values
    xt = np.zeros(len(x))
    if np.count_nonzero(x) == 0:
        print("only zero valued values found\n")
        return x
    
    xt = transform_to_normal(x)
    
    xMean, xStd = np.mean(xt), np.std(xt)
    outliers = np.where(xt > xMean + stdFactor*xStd)[0]
    inliers = np.where(xt <= xMean + stdFactor*xStd)[0]
    if len(outliers) > 0:
        print("found outlier with factor: "+str(stdFactor)+" : "+str(outliers))
        xinline = x[inliers]
        maxInRange = np.max(xinline)
        print("replacing outliers {} with max={}".format(outliers,maxInRange))
        vals = x.copy()
        vals[outliers] = maxInRange
    return x

cleanFeatures = features.apply(replace_outliers_with_limit)
cleanFeatures.head(10)

# COMMAND ----------

import scipy.stats as st

def extract_cummulative_prob_from_dist(x):
    
    print("extracting commulative probs for series:",x.name,"length",len(x))
    #print(x.head())
    
    xPositive = x[x>0]
    xNonPositive = x[x<=0]
    
    ratioOfZeroEvents = len(xNonPositive)/len(x)
    probs = np.zeros(len(x))
    if(len(xPositive)>0):
        params = st.expon.fit(xPositive)
        arg = params[:-2]
        loc = params[-2]
        scale = params[-1]
        #print('params = {}, {}, {}.'.format(arg,loc,scale))
        probs[x>0] = st.expon.cdf(xPositive, loc=loc, scale=scale, *arg)

    return probs
  
cdfs = cleanFeatures.apply(extract_cummulative_prob_from_dist).add_suffix("_CDF")
cleanFeatures

# COMMAND ----------

from scipy.stats import rankdata

NUM_OF_FEATURES = 3.0

cdfs['metric'] = cdfs.sum(axis = 1) / NUM_OF_FEATURES
cdfs = cdfs.sort_values('metric')
cdfs['rank'] = cdfs.metric.rank(method="min")/len(cdfs)*1.0

finalDF = spark.createDataFrame(cdfs.reset_index())

# COMMAND ----------

display(finalDF)

# COMMAND ----------

display(finalDF)