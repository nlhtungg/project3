from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("CreateUnitRelationTable").getOrCreate()

    spark.sql("""
    INSERT OVERWRITE TABLE gold.unit_relation
    WITH top4_matches AS (
    SELECT 
        match_id, 
        puuid
    FROM silver.tft_match_participants
    WHERE placement <= 4
    ),
    unique_units AS (
    SELECT DISTINCT
        u.match_id,
        u.puuid,
        u.character_id
    FROM silver.tft_participant_units u
    JOIN top4_matches t ON u.match_id = t.match_id AND u.puuid = t.puuid
    )
    SELECT 
        a.character_id AS source_node, 
        b.character_id AS target_node, 
        COUNT(*) AS weight             
    FROM unique_units a
    JOIN unique_units b 
        ON a.match_id = b.match_id 
        AND a.puuid = b.puuid
    WHERE a.character_id < b.character_id
    GROUP BY 
        a.character_id, 
        b.character_id
    ORDER BY weight DESC
    """)

    # Đọc và hiển thị kết quả
    df = spark.sql("SELECT * FROM gold.unit_relation")
    df.show()

if __name__ == "__main__":
    main()