"""

The next step is to read read the documents' tables
We will read the documents from the departments

Land Admin
"""

"""
import spark session from read_db file
Import configurations from the config file
"""
import read_db

import config

docs_db_connection = read_db.spark.read \
    .format("jdbc") \
    .option("url", config.documents_db_url) \
    .option("user", config.db_user) \
    .option("password", config.db_password) \
    .option("driver", "org.postgresql.Driver")
"""
import the routing table to extend
"""

admin_routing = read_db.combined_routing_table

"""Read all the land admin docs of interest"""
for tablename in config.land_admin_documents:
    document_df = docs_db_connection.option("dbtable", f"{config.admin_schema}.{tablename}") \
        .load()
    "print the shapes of the tables"
    # print(f"{tablename}: ", (document_df.count(), len(document_df.columns)))

    """
    Rename columns for each of the datasets to include the table name as the suffix
    For example, the id column in the lease_fowarding_form table will be id_lease_fowarding_form
    
    """
    for column_name in document_df.columns:
        document_df = document_df.withColumnRenamed(f"{column_name}", f"{column_name}_{tablename}")

    """
    Join all the documents with the routing table
    We will use full outer join to capture all possible scenarios for the data
    We will simply extend the routing table
    """

    admin_routing = admin_routing.join(document_df,
                                       admin_routing.land_admin_parcel_no == f"{document_df}.file_number_{tablename}",
                                       'fullouter')
    """
    In case you will need the specific databases at this point
    """
    # globals()[tablename] = document_df

# print(admin_routing.columns)

"""Rename the table the Admin Table to reflect a better approach"""
admin_table = admin_routing
