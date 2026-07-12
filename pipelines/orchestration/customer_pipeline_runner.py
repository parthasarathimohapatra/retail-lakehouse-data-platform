""" Orchestrate the customer batch-processing pipeline."""

from src.common.spark_session_manager import create_spark_session
from src.ingestion.customer_ingestion import load_customer_dataset
from src.validations.customer_data_validator import (
    print_validation_report,
    validate_customer_dataset,
)

def main() -> None:
    spark = create_spark_session(
        application_name = "RetailCustomerBatchPipeline"
    )
    try:
        print("Starting customer batch pipeline ...")
        customer_dataframe = load_customer_dataset(spark)

        print("Customer dataset loaded successfully.")
        customer_dataframe.printSchema()
        customer_dataframe.show(10, truncate=False)

        validation_result = validate_customer_dataset(customer_dataframe)
        print_validation_report(validation_result)

        if not validation_result.is_valid:
            raise ValueError(
                "Customer validation failed. "
                "Bronze processing has been stopped."
            )
        print("Customer pipeline completed successfully.")
    except Exception as error:
        print(f"Customer pipeline failed: {error}")
        raise
    finally:
        spark.stop()
        print("SparkSession Stopped.")

if __name__ == "__main__":
    main()

