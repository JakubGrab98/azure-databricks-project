"""Helper functions used in silver layer transformations."""
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from typing import List


def convert_to_array(df: DataFrame, columns: List[str], array_type: str) -> DataFrame:
    """
    Converts stringified lists (e.g. "[a, b, c]") to Spark arrays for given columns.

    Args:
        df (DataFrame): Input DataFrame.
        columns (List[str]): List of column names to transform.
        array_type (str): Spark array type to cast to (e.g. 'array<string>').

    Returns:
        DataFrame: DataFrame with specified columns converted to arrays.
    """
    for column in columns:
        df = df.withColumn(
            column,
            f.expr(f"split(regexp_replace({column}, '^\[|\]$', ''), ',')").cast(array_type)
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
