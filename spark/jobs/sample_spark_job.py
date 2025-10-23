from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("SampleSparkJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Create a sample DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Cathy", 29)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    # Perform a simple transformation
    df_filtered = df.filter(df.Age > 30)

    # Write the results to a Delta table
    df_filtered.write \
        .format("delta") \
        .mode("overwrite") \
        .save("s3a://bronze/sample_delta_table/")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()