from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("CreateTraitPickPlaceTable").getOrCreate()

    spark.sql("""
    CREATE OR REPLACE TABLE gold.trait_pick_place
    USING DELTA
    LOCATION 's3a://gold/trait_pick_place'
    AS
    WITH 
    clean_units AS (
        SELECT 
            unit_id,
            unit_name,
            trait
        FROM (
            SELECT 
                unit_id, 
                unit_name, 
                trait1, trait2, trait3, trait4
            FROM silver.tft_units
            WHERE set = 16 
            AND is_current = true 
        ) tmp
        LATERAL VIEW EXPLODE(ARRAY(trait1, trait2, trait3, trait4)) t AS trait
        WHERE trait IS NOT NULL AND trait <> ''
    ),

    player_distinct_units AS (
        SELECT DISTINCT
            match_id,
            puuid,
            character_id 
        FROM silver.tft_participant_units
    ),

    player_traits_calculated AS (
        SELECT 
            p.match_id,
            p.puuid,
            u.trait,
            COUNT(p.character_id) AS unit_count 
        FROM player_distinct_units p
        JOIN clean_units u ON p.character_id = u.unit_id
        GROUP BY p.match_id, p.puuid, u.trait
    ),

    match_results AS (
        SELECT 
            match_id,
            puuid,
            placement
        FROM silver.tft_match_participants
    ),

    final_dataset AS (
        SELECT 
            pt.match_id,
            pt.puuid,
            pt.trait,
            pt.unit_count,
            mr.placement
        FROM player_traits_calculated pt
        JOIN match_results mr ON pt.match_id = mr.match_id AND pt.puuid = mr.puuid
    )

    SELECT 
        f.trait,
        COUNT(DISTINCT CONCAT(f.match_id, f.puuid)) AS total_picks,
        CAST(AVG(f.placement) AS DECIMAL(10, 2)) AS avg_placement,
        CAST(SUM(CASE WHEN f.placement <= 4 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) AS top4_rate
    FROM final_dataset f
    GROUP BY f.trait
    ORDER BY total_picks DESC;
    """)

    # Đọc và hiển thị kết quả
    df = spark.sql("SELECT * FROM gold.trait_pick_place")
    df.show()

if __name__ == "__main__":
    main()