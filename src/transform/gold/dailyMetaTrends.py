from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("DailyMetaTrendsTable").getOrCreate()

    spark.sql("""
    INSERT OVERWRITE TABLE gold.daily_meta_trends
    PARTITION (report_date)
    WITH daily_matches AS (
        SELECT 
            DATE(processed_ts) AS report_date,
            COUNT(DISTINCT match_id) AS total_matches
        FROM silver.tft_match_participants
        WHERE processed_ts IS NOT NULL
        GROUP BY 1
    ),  
    daily_units AS (
    SELECT 
        DATE(m.processed_ts) AS report_date,
        ELEMENT_AT(SPLIT(u.character_id, '_'), -1) AS character_id, 
        COUNT(*) AS pick_count,
        AVG(m.placement) AS avg_placement
    FROM silver.tft_participant_units u
    JOIN silver.tft_match_participants m 
        ON u.match_id = m.match_id 
        AND u.puuid = m.puuid
    WHERE u.character_id LIKE 'TFT16%'
    GROUP BY 1, 2
    )
    SELECT 
        u.character_id,
        u.pick_count,
        u.avg_placement,
        m.total_matches,
        CAST(u.pick_count AS DOUBLE) / (m.total_matches * 8) AS pick_rate,
        u.report_date
    FROM daily_units u
    JOIN daily_matches m ON u.report_date = m.report_date
    """)

    # Đọc và hiển thị kết quả
    df = spark.sql("SELECT * FROM gold.daily_meta_trends")
    df.show()

if __name__ == "__main__":
    main()