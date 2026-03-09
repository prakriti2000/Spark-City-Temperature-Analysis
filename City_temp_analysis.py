from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count, col
from pyspark.sql.types import StructType,StructField,StringType,DoubleType

if __name__ == "__main__":
    #creating a spark session called car analysis
    spark = SparkSession.builder.appName("CarAnalysis").getOrCreate()

    # data provided
    data = [
        ("New York", 10.0),
        ("New York", 12.0),
        ("Los Angeles", 20.0),
        ("Los Angeles", 22.0),
        ("San Francisco", 15.0),
        ("San Francisco", 18.0)
    ]

    schema = StructType([
        StructField("city", StringType()),
        StructField("temperature",DoubleType())
    ])

    df = spark.createDataFrame(data=data, schema=schema)

    print ("Original Data")
    df.show()

    result = df.groupBy("city").agg(
        avg("temperature").alias("avg_temperature"),
        sum("temperature").alias("total_temperature"),
        count("temperature").alias("num_measurements")
    )

    filtered = result.filter(col("total_temperature") > 30)

    final_result = filtered.orderBy("city")

    print("Final Result")
    final_result.show()