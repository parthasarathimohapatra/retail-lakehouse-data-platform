"""Data-quality validations for the customer dataset."""
from dataclasses import dataclass
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum, trim
from typing import Dict

EXPECTED_CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
]

@dataclass(frozen=True)
class CustomerValidationResult:
    total_records: int
    duplicate_customer_ids: int
    null_counts: Dict[str, int]
    empty_string_counts: Dict[str, int]
    schema_valid: bool

    @property
    def is_valid(self) -> bool:
        return (
            self.total_records > 0
            and self.duplicate_customer_ids == 0
            and all(value == 0 for value in self.null_counts.values())
            and all(value == 0 for value in self.empty_string_counts.values())
            and self.schema_valid
        )

def validate_customer_dataset(customer_dataframe):
    """Run customer data-quality checks and return the results.total"""
    total_records = customer_dataframe.count()

    schema_valid = (
        customer_dataframe.columns == EXPECTED_CUSTOMER_COLUMNS
    )

    null_counts_row = customer_dataframe.select(
        *[
            sum(col(column_name).isNull().cast("int")).alias(column_name)
            for column_name in customer_dataframe.columns
        ]
    ).first()

    null_counts_dictionary = null_counts_row.asDict()

    null_counts = {
        column_name: int(null_counts_dictionary.get(column_name, 0) or 0)
        for column_name in customer_dataframe.columns
    }

    duplicate_customer_ids = (
        customer_dataframe.groupBy("customer_id")\
                          .count()\
                          .filter(col("count") > 1)
                          .count()
    )
     
    string_columns = ["customer_id", "customer_city", "customer_state"]

    empty_counts_row = customer_dataframe.select(
        *[
            sum(
                (trim(col(column_name))== "").cast("int")
            ).alias(column_name)
            for column_name in string_columns
        ]
    ).first()

    empty_string_counts = {
        column_name: int(empty_counts_row[column_name] or 0)
        for column_name in string_columns
    }
    
    return CustomerValidationResult(
        total_records = total_records,
        duplicate_customer_ids = duplicate_customer_ids,
        null_counts = null_counts,
        empty_string_counts = empty_string_counts,
        schema_valid = schema_valid,
    )

def print_validation_report(validation_result : CustomerValidationResult):
    """Print a readable customer validation report."""
    status = "SUCCESS" if validation_result.is_valid else "FAILED"

    print("\n" + "=" * 60)
    print("CUSTOMER DATASET VALIDATION REPORT")
    print("=" * 60)
    print(f"Total records               : {validation_result.total_records}")
    print(f"Schema validation           : "
          f"{'PASS' if validation_result.schema_valid else 'FAIL'}")
    print(
        f"Duplicate customer IDs        :"
        f"{validation_result.duplicate_customer_ids}"
    )

    print("\nNull counts:")
    for column_name, count in validation_result.null_counts.items():
        print(f"  {column_name:<30}: {count}")

    print("\nEmpty-string counts:")
    for column_name, count in validation_result.null_counts.items():
        print(f"  {column_name:<30}: {count}:")

    print(f"\nOverall validation status : {status}")
    print("=" * 60)