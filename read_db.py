"""
Import os to enable compatibility with the host OS
Import Pyspark, a Spark Python API
Import lit to create columns with null value inputs
The config file has the usernames and passwords to my db
config file also contains database connection urls in the format: "jdbc:postgresql://IP:port/db_namecd"
"""

import os

import config

import pyspark

from pyspark.sql.functions import lit

"Configure the location of spark"
os.environ['SPARK_HOME'] = "/opt/spark"

"Create Spark Application"
appName: str = "PySpark PostgreSQL Test - via jdbc"
master = "local"

"Create and instantiate a spark session"
spark = pyspark.sql.SparkSession.builder.master(master) \
    .appName(appName) \
    .config("spark.jars", config.spark_jars_config) \
    .getOrCreate()

"Create Connection to the Database"

rt_db_connection = spark.read \
    .format("jdbc") \
    .option("url", config.data_mart_url) \
    .option("user", config.db_user) \
    .option("password", config.db_password) \
    .option("driver", "org.postgresql.Driver")

"Extract the names of tables from information schema"
table_names = rt_db_connection.option("dbtable", "information_schema.tables")\
    .load() \
    .filter("table_schema = 'routing_schema'")\
    .select("table_name")

"Exclude the the combined routing table from the list of tables"
table_names = table_names.filter(table_names.table_name != 'routing_combined')

"Create a list of table names from the table_names dataframe above"
table_names_list = [row.table_name for row in table_names.collect()]

"""Read the first Routing Table in the Routing Table List"""
first_routing_table = rt_db_connection.option("dbtable", f"routing_schema.{table_names_list[0]}").load()

"""Update the first routing table above by concatenating it with the rest of the routing tables in the database"""
for tablename in table_names_list[1:]:
    routing_tables_df = rt_db_connection.option("dbtable", f"routing_schema.{tablename}") \
        .load()

    """"
        Spark only allows a union of the same number of columns
        Some Dataframes have a lower number of columns than the first dataset. 
        We will add the missing columns to the dataframes we are looping through
        We will then enter the value 'None' on the empty columns
        Newly added columns contains null values and we add constant column using lit() function
    """

    for column in [column for column in first_routing_table.columns if column not in routing_tables_df.columns]:
        routing_tables_df = routing_tables_df.withColumn(column, lit(None))

    """
        The resulting dataframes are still not equal in columns
        We will add the existing columns in the second dataframe to the first dataframe
    """

    for column in [column for column in routing_tables_df.columns if column not in first_routing_table.columns]:
        first_routing_table = first_routing_table.withColumn(column, lit(None))

    """
    Incrementally append the newly created read dataframes to the first dataframe created outside this loop
    """
    first_routing_table = first_routing_table.unionByName(routing_tables_df)
    # print(f"Added db {tablename}: ", (first_routing_table.count(), len(first_routing_table.columns)))

    """
    In case you want to see anything about the individual dataframes, uncomment this part then call the specific 
    dataframes 
    """
    # globals()[tablename] = routing_tables_df
"""
rename the combined dataframe for convenience
"""
combined_routing_table = first_routing_table
