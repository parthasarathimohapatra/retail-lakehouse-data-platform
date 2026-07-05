from pyspark.sql import SparkSession

def create_spark_session():
    return (SparkSession.builder\
                    .appName("RetailLakehouse")\
                    .master("local[*]")\
                    .getOrCreate())

def load_customer_dataset(spark):
    customer_file_path = "datasets/raw/olist_customers_dataset.csv"
    return (
        spark.read.option("header",True)
                  .option("inferSchema",True)
                  .csv(customer_file_path)
    )

def main():
    spark = create_spark_session()
    customer_dataframe = load_customer_dataset(spark)
    customer_dataframe.printSchema()
    customer_dataframe.show(50,truncate=False)
    total_customer_count = customer_dataframe.count()
    print(f"Total Customer Records: {total_customer_count}")

    spark.stop()


if __name__ == "__main__":
    main()

