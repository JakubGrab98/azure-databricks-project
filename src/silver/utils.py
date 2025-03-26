"""Helper functions used in silver layer transformations."""
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import List


def convert_to_array(df: DataFrame, columns: List[str], array_type: str = "array<string>") -> DataFrame:
    """
    Converts stringified lists (e.g. "['a', 'b']") to Spark arrays.

    Args:
        df (DataFrame): Input DataFrame.
        columns (List[str]): List of column names to convert.
        array_type (str): Spark array type. Defaults to "array<string>".

    Returns:
        DataFrame: DataFrame with columns converted to arrays.
    """
    for column in columns:
        df = df.withColumn(
            column,
            f.split(
                f.regexp_replace(f.col(column), r"[\[\]'\" ]", ""), ","
            ).cast(array_type)
        )
        df = df.withColumn(
            column,
            f.expr(f"filter({column}, x -> x is not null and x != '')")
        )
    return df

def remove_quotation(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Removes leading and trailing double quotes from string columns.

    Args:
        df (DataFrame): Input DataFrame.
        columns (List[str]): Column names to clean.

    Returns:
        DataFrame: Cleaned DataFrame.
    """
    for column in columns:
        df = df.withColumn(
            column,
            f.trim(f.regexp_replace(column, '^"|"$', ''))
        )
    return df
