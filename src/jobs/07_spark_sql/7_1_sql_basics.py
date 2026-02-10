from src.utils.spark_helper import get_spark_session, get_db_properties


def main():
    # 1. Initialize Spark Session using the helper
    spark = get_spark_session("SparkSQL Basics")
    db_config = get_db_properties()

    # 2. Load Sample Data (dim_aircraft table in Postgres)

    aircraft_df = spark.read.jdbc(
        url=db_config.url, table="dim_aircraft", properties=db_config.properties
    )

    # 3. Register Temporary View
    # This makes the DataFrame accessible via SQL
    aircraft_df.createOrReplaceTempView("v_aircraft")

    print("--- SQL Query: Basic Selection ---")
    # 4. Basic SQL Query
    basic_sql = """
        select tail_number, aircraft_type, total_flight_hours
        from v_aircraft
        where total_flight_hours > 1000
        order by total_flight_hours desc;
    """
    spark.sql(basic_sql).show()

    print("--- SQL Query: Aggregation with CTE ---")
    # 5. SQL Aggregation with CTE
    agg_sql = """
        with type_summary as (
            select
                aircraft_type,
                sum(total_flight_hours) as total_hours,
                count(*) as plane_count
            from v_aircraft
            group by aircraft_type
        )
        select * from type_summary where plane_count > 5
    """
    agg_result = spark.sql(agg_sql)
    agg_result.show()

    print("--- Catalyst Optimizer ---")
    # See how did the SQL been transformed
    agg_result.explain()

    print("--- Catalog Metadata ---")
    # 6. Check existing tables/views in the catalog
    spark.catalog.listTables()
    for table in spark.catalog.listTables():
        print(f"Table Name: {table.name:20} | IsTemporary: {table.isTemporary}")


if __name__ == "__main__":
    main()
