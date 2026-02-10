from src.utils.spark_helper import get_spark_session, get_db_properties


def main():
    # 1. Initialize Spark Session using the helper
    spark = get_spark_session("SparkSQL Basics")
    db_config = get_db_properties()

    # 2. Load Sample Data (dim_aircraft table in Postgres)

    aircraft_df = spark.read.jdbc(
        url=db_config.url, 
        table="dim_aircraft", 
        properties=db_config.properties
    )

    # 3. Register Temporary View
    # This makes the DataFrame accessible via SQL
    aircraft_df.createOrReplaceTempView("view_aircraft_logs")

    print("--- SQL Query: Basic Selection ---")
    # 4. Basic SQL Query
    sql_result = spark.sql(
        """
        select tail_number, aircraft_type
        from view_aircraft_logs;
    """
    )
    sql_result.show()

    print("--- SQL Query: Aggregation ---")
    # 5. SQL Aggregation (Compare this with df.groupBy syntax)
    agg_result = spark.sql(
        """
        select aircraft_type, sum(total_flight_hours) as flight_hours
        from view_aircraft_logs
        group by aircraft_type
        order by flight_hours desc;
    """
    )
    agg_result.show()

    print("--- Catalog Metadata ---")
    # 6. Check existing tables/views in the catalog
    spark.catalog.listTables()
    for table in spark.catalog.listTables():
        print(f"Table Name: {table.name}, IsTemporary: {table.isTemporary}")


if __name__ == "__main__":
    main()
