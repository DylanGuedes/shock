from pyspark.sql.types import *
from math import sin, cos, sqrt, atan2, radians
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofyear, minute, array, udf, collect_list, explode, mean, get_json_object, from_json, size
from pyspark.sql.types import *
from datetime import datetime
from xml.dom import minidom
import requests
from pyspark.sql.window import Window
import json
import builtins
from pyspark.sql import DataFrameStatFunctions as statFunc
import numpy as np
import pyspark.sql.functions as func


MAD_CONSTANT = 1.4826
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def valueMinusMean(values, mean_val):
    # OBS: enumerate does not reset if it is calld multiple times
    for i, u in enumerate(values):
        values[i] = abs(u - mean_val)
    return values


def getSchema():
    return StructType([
        StructField("uuid", StringType(), False),
        StructField("capabilities", StructType([
            StructField("edge_monitoring", ArrayType(
                StructType([
                    StructField("origin", StructType([
                        StructField("lat", StringType(), True),
                        StructField("lon", StringType(), True)
                    ])),
                    StructField("destination", StructType([
                        StructField("lat", StringType(), True),
                        StructField("lon", StringType(), True)
                    ])),
                    StructField("avg_speed", DoubleType(), True),
                    StructField("ref_hour", DoubleType(), True),
                    StructField("date", StringType(), True)
                ])
            ))
        ]))
    ])


def median(values_list):
    med = np.median(values_list)
    return float(med)


def extractJsonFromString(df, attrs):
    json_objects = []
    for u in attrs:
        json_objects.append(get_json_object(df.value, '$.'+u).alias(u))
    return json_objects


udfMedian = func.udf(median, FloatType())


def calculatesMad(df, c):
    w = Window()\
            .partitionBy("origin", "destination", "ref_hour")

    return df\
        .withColumn("array({0})".format(c), func.collect_list(col(c)).over(w))\
        .withColumn("median({0})".format(c), udfMedian(col("array({0})".format(c))))\
        .withColumn(
            "{0}-median({0})".format(c), udfValueMinusMean(
                col("array({0})".format(c)), col("median({0})".format(c))
            )
        )\
        .withColumn("median({0}-median({0}))".format(c), udfMedian(col("{0}-median({0})".format(c))))\
        .withColumn("mad", col("median({0}-median({0}))".format(c)) * MAD_CONSTANT)


def checkThresholds(measure, upper, lower):
    return (measure > upper) or (measure < lower)


def publish_anomalies(anomalies):
    collection = anomalies.rdd.collect()
    mylist = []
    for u in collection:
        myhash = {
            "locations": [
                { "lat": float(u["origin_lat"]), "lng": float(u["origin_lon"])},
                { "lat": float(u["dest_lat"]), "lng": float(u["dest_lon"]) }
            ],
            "hour": u["ref_hour"],
            "date": u["date"].strftime("%Y-%m-%d"),
            "avg_speed": u["avg_speed"]
        }

        mylist.append(myhash)
        if (len(mylist) > 50):
            r = requests.post("http://192.168.0.105:8000/data/bulk", json={
                "collection": "anomaly",
                "data": mylist
            })
            mylist = []


if __name__ == '__main__':
    udfValueMinusMean = udf(valueMinusMean, ArrayType(DoubleType()))
    udfIsAnomaly = udf(checkThresholds, BooleanType())

    # get data from collector
    collector_url = "http://data-collector:3000"
    r = requests.post(collector_url + '/resources/data', json={"capabilities": ["edge_monitoring"]})
    resources = r.json()["resources"]
    rdd = spark.sparkContext.parallelize(resources)
    df = spark.createDataFrame(resources, getSchema())

    # cleanning the data and calculating mad
    clean_data = df\
            .select("uuid", explode(col("capabilities.edge_monitoring")).alias("values"))\
            .select("uuid", "values.avg_speed", "values.ref_hour", "values.date", "values.origin", "values.destination")

    clean_data.show()

    velocity_data = calculatesMad(clean_data, "avg_speed")
    velocity_data.show()

    mad_data = velocity_data\
            .withColumn("upper_threshold", col("median(avg_speed)") + 3.*col("mad"))\
            .withColumn("lower_threshold", col("median(avg_speed)") - 3.*col("mad"))

    mad_data.select("origin", "destination", "ref_hour", "mad", "upper_threshold", "lower_threshold").show(truncate=False)
    anomalies = mad_data\
            .withColumn("is_anomaly", udfIsAnomaly(col("avg_speed"), col("upper_threshold"), col("lower_threshold")))

    anomalies.select("is_anomaly", "origin", "destination").show(truncate=False)
    anomalies.select("is_anomaly").groupBy("is_anomaly").count().show()

    publish_anomalies(anomalies)
