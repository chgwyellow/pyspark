from src.utils.spark_helper import get_spark_session, get_db_properties


def main():
    spark = get_spark_session("SparkSQL Complex Logic")
    db_config = get_db_properties()

    # Load dim_aircraft table and register to view
    aircraft_df = spark.read.jdbc(
        url=db_config.url, table="dim_aircraft", properties=db_config.properties
    )

    aircraft_df.createOrReplaceTempView("v_aircraft")

    complex_sql = """
        with fleet_status as (
            select
                tail_number,
                aircraft_type,
                total_flight_hours,
                case
                    when total_flight_hours >= 2500 then 'A-Check Due (Immediate)'
                    when total_flight_hours between 1500 and 2499 then 'Monitor Closely'
                    else 'Optimal Condition'
                end as maintenance_status,
                upper(aircraft_type) as type_upper,
                date_format(current_date(), 'yyyy-mm-dd') as report_date
            from v_aircraft
        ),
        summary_stats as (
            select
                maintenance_status,
                count(*) as aircraft_count,
                round(avg(total_flight_hours), 1) as avg_hours,
                max(total_flight_hours) as max_hours
            from fleet_status
            group by maintenance_status
        )
        -- order by average time
        select * from summary_stats
        order by avg_hours desc
    """

    print("\n--- Complex Logic Result for Fleet Maintenance Summary---")
    result_df = spark.sql(complex_sql)
    result_df.show()

    print("\n--- (Physical Plan) ---")
    result_df.explain()


if __name__ == "__main__":
    main()
