from datetime import date, timedelta
from pyspark.sql.functions import lit, expr, col
from pyspark.sql import DataFrame
import re

def create_status_dict(tables_list: list) -> list:
    """
    Overview:
        Gets the ingestion status of the given H360 table.
    Parameters:
        tables_list (list): A list of tuples containing table name and folder name.
    Return:
        status_dict (list): The filtered h360_status table converted to a list of dictionaries.
    """

    table_filter = []

    for table, folder in tables_list:
        table_filter.append(table)

    h360_status = spark.read.table("<placeholder catalog>.<placeholder schema>.h360_status") \
                            .filter((col('status') == 'incomplete') & (col('table_name').isin(table_filter))) \
                            .orderBy(col('table_name'), col('start_date'))

    status_dict = [row.asDict() for row in h360_status.collect()]

    return status_dict


def generate_dates(start_date, end_date):

    dates_list = spark.read.table("systematic.calendar.calendar").filter(
        col('calendar_date') >= start_date 
        & col('calendar_date') <= end_date
        & col('is_business_day') == 1
    ).rdd.map(lambda row: row['calendar_date']).collect()

    return dates_list


def process_table(folder: str, table: str, start_date: date, end_date: date) -> list:
    """
    Overview:
        This function reads in parquet files over a date range and appends dataframes into a list. 
    Parameters:
        folder (str): The folder where the table resides.
        table (str): The table name.
        start_date (date): The start date of the date range.
        end_date (date): The end date of the date range.
    Returns:
        dataframes (list): A list of dataframes to be unioned before inserting into delta table.
    """

    day = start_date
    dataframes = []
    path = '/mnt/<placeholder cloud storage container>'

    target_df = spark.table(delta_table)
    target_schema = target_df.schema
    target_columns = set(target_df.columns)

    for day in dates_list:
        day_str = day.strftime('%Y/%m/%d')
        try:
            df = spark.read.parquet(f"{path}/{folder}/{table}/{day_str}/*.parquet")
            df = df.withColumn('utc_timestamp', lit(day))
            df = clean_columns(df)
            df_columns = set(df.columns)
            dataframes.append(df)
        except Exception as e:
            pass

    return dataframes


def clean_columns(df: DataFrame) -> DataFrame:
    """
    Overview:
        Cleans column names because Unity Catalog does not like special characters.
    Parameters:
        df (DataFrame): Dataframe to be cleaned.
    Return:
        final_df (DataFrame): Cleaned dataframe with all columns converted to string.
    """

    new_columns = [re.sub(r'[^a-zA-Z0-9_]', '_', c) for c in df.columns]

    cleaned_df = df.toDF(*new_columns)

    final_df = cleaned_df.select([col(c).cast("string").alias(c) for c in df.columns])

    return final_df


def update_unity(df: DataFrame) -> int:
    """
    Overview:
        Create delta table in Unity Catalog for the specified H360 table with the given dataframe.
    Parameters:
        df (DataFrame): Dataframe of the given H360 table.
    Return:
        0 to signify success.
    """
    
    delta_table = f"<placeholder catalog>.<placeholder schema>.{table}"

    df.write \
        .mode('append') \
        .format('delta') \
        .partitionBy("utc_timestamp") \
        .saveAsTable(delta_table)


    return 0


def update_status(table: str, start_date: date, today: date) -> int:
    """
    Overview:
        Updates the h360_status to complete.
    Parameters:
        table (str): Name of table to be edited.
        start_date (date): Start of date interval in current chunk.
        today (date): Today's date.
    Return:
        0 to signify success.
    """

    spark.sql(f"""
        UPDATE laboratory.linh.h360_status
        SET status = 'complete', run_date = '{today}'
        WHERE table_name = '{table}'
        AND start_date = '{start_date}'
    """)


    return 0
