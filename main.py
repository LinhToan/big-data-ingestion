# This was used for the initial ingestion for H360 parquet files to Unity Catalog. 
# The strategy is to read the respective daily parquet for each day over the specified time interval (chunk), append each dataframe to a list, 
# then union them all together to create one big dataframe to be inserted into Unity Catalog. 
# This strategy takes advantage of Spark's big data processing. Another delta table was created to keep track of progress, 
# that way if the notebook ever crashes we would be able to run again as-is as the code would process the next table or time interval, 
# due to the respective table's status getting updated to complete after insert.

from datetime import date
from functools import reduce
from helpers import create_status_dict, generate_dates, process_table, clean_columns, update_unity, update_status


def main(status_dict: dict) -> int:
    """
    Overview:
        Main function that processes parquet files over a date range, 
        unions them all together to create one big dataframe, then appends said dataframe into its respective delta table.
        This takes advantage of Spark's ability to handle large volumes of data efficiently.
    Parameters:
        status_dict: Dictionary containing ingestion status for each table. 
    Returns:
        Return 0 to signify success.
    """

    # Enable schema evolution
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    num_cores = spark.sparkContext.defaultParallelism

    status_dict = create_status_dict(tables_list)

    for chunk in status_dict:
        if chunk['status'] == 'complete':
            continue

        table, folder, start_date, end_date = chunk['table_name'], chunk['folder'], chunk['start_date'], chunk['end_date']

        dataframes = process_table(folder, table, start_date, end_date)

        if not dataframes:
            update_status(table, start_date, date.today())
            print(f"No data found for {table} between {start_date} and {end_date}.")
            continue

        df = reduce(lambda x, y: x.unionByName(y, allowMissingColumns=True), dataframes).repartition(num_cores)

        update_unity(df, table)
        update_status(table, start_date, date.today())
        print(f"{table} is processed and status is updated for {start_date} to {end_date}.")

    return 0


if __name__ == "__main__":

    h360_tables = spark.read.csv("/mnt/<placeholder for cloud storage directory>.csv", header=True)

    tables_list = [
        (row['table_name'], row['existing_path'].rpartition('/')[0])
        for row in h360_tables.collect()
    ]

    main(tables_list)

    for table, folder in tables_list:
        if spark.catalog.tableExists(f"<placeholder catalog>.<placeholder schema>.{table}"):
            spark.sql(f"OPTIMIZE <placeholder catalog>.<placeholder schema>.{table}")
            print(f"Optimized {table}.")