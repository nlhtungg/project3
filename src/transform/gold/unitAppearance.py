from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("CreateUnitAppearanceTable").getOrCreate()

    spark.sql("""
    insert overwrite table gold.unit_appearance
    WITH base AS (
        SELECT
            match_id,
            puuid,
            split_part(character_id, '_', 2) AS character_id
        FROM silver.tft_participant_units
        WHERE character_id like 'TFT16%'
    ),
    dedup AS (
        SELECT match_id, puuid, character_id
        FROM base
        GROUP BY match_id, puuid, character_id   -- dedupe step
    )
    SELECT
        character_id,
        COUNT(*) AS count
    FROM dedup
    GROUP BY character_id
    """)

    # Đọc và hiển thị kết quả
    df = spark.sql("SELECT * FROM gold.unit_appearance")
    df.show()

if __name__ == "__main__":
    main()