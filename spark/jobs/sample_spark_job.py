from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SampleSparkJob") \
        .getOrCreate()

    # Create a sample DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 39)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Perform a simple transformation
    df_filtered = df.filter(df.Age > 30)

    # Write the results to MinIO
    df_filtered.write \
        .format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save("s3a://bronze/sample_output/")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()