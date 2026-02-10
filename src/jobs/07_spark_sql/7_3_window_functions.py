from src.utils.spark_helper import get_spark_session, get_db_properties


def main():
    # Initialize
    spark = get_spark_session("SparkSQL Window Functions")
    db_config = get_db_properties()

    # Load data and create a temp view
    aircraft_df = spark.read.jdbc(
        url=db_config.url, table="dim_aircraft", properties=db_config.properties
    )

    aircraft_df.createOrReplaceTempView("v_aircraft")

    # Window function
    # Group by aircraft_type and rank by hours
    # calculate the current aircraft type flight hour gap with the max one
    # Count the total aircraft type
    window_sql = """
        select
            tail_number,
            aircraft_type,
            total_flight_hours,
            -- Rank the top hours
            Rank() over(
                partition by aircraft_type 
                order by total_flight_hours desc
            ) as hours_rank,
            -- Top hour in every type
            MAX(total_flight_hours) over(
                partition by aircraft_type
            ) as type_max_hours,
            -- Difference
            (MAX(total_flight_hours) over(
                partition by aircraft_type) - total_flight_hours
            ) as hours_gap,
            -- Count aircraft
            count(*) over(
                partition by aircraft_type
            ) as type_fleet_count
        from v_aircraft
    """

    print("\n --- Rank via hours by aircraft types ---")
    result_df = spark.sql(window_sql)
    result_df.show(truncate=False)

    print("\n --- Planning Analysis (Window Step) ---")
    result_df.explain()


if __name__ == "__main__":
    main()
