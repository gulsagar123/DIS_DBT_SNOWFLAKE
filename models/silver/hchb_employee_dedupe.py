import json
import datetime
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import lit, when, expr, col, current_timestamp, current_user
from snowflake.snowpark.window import Window
from snowflake.snowpark.types import StructType, StructField, StringType, ArrayType, VariantType

def pairing(session, intermediate_db_name, intermediate_schema_name, source_name, input_query, pairing_on, remove_values, window_size):
    fields_list = session.sql(input_query).columns

    varchar_upper_pattern = [
        f"UPPER(NVL({field}::VARCHAR, 'UNDEFINED')) AS {field}"
        for field in fields_list if field != 'ID'
    ]
    fields_as_varchar_upper = ', '.join(varchar_upper_pattern)

    name_expr = (
        "UPPER(REGEXP_REPLACE("
        "CONCAT("
        "COALESCE(NULLIF(FIRST_NAME, ''), 'UNDEFINED'), "
        "COALESCE(NULLIF(LAST_NAME,  ''), 'UNDEFINED')"
        "), '[^a-zA-Z]', ''))"
    )

    df = session.sql(f"""
        SELECT
            ROW_NUMBER() OVER (ORDER BY ID)  AS PK,
            ID,
            {fields_as_varchar_upper},
            {name_expr}                      AS NAME,
            ROW_NUMBER() OVER (
                ORDER BY {name_expr}, ID
            )                                AS SORT_INDEX
        FROM ({input_query})
    """)

    df.write.mode('overwrite').save_as_table(
        f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_DEDUPE_INPUT"
    )

    stage_schema = StructType([
        StructField(field.name, ArrayType(StringType()), nullable=field.nullable)
        for field in df.schema.fields
    ])
    
    existing_fields = [f.name for f in stage_schema.fields]
    if 'FIRST_LAST_NAME' not in existing_fields:
        stage_schema = stage_schema.add(field='FIRST_LAST_NAME', datatype=ArrayType(StringType()))
    
    stage_schema = stage_schema.add(field='PAIRED_ON',       datatype=StringType())
    stage_schema = stage_schema.add(field='PAIRING_METHOD',  datatype=StringType())

    session.create_dataframe([], schema=stage_schema) \
           .write.mode('overwrite') \
           .save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_PAIRS_STAGE")

    for pairing_field, method in pairing_on.items():
        values_to_remove = remove_values.get(pairing_field, "''")

        col_list = [f"ARRAY_CONSTRUCT(DF1.{c}, DF2.{c}) AS {c}" for c in df.columns]
        if 'FIRST_LAST_NAME' not in df.columns:
            col_list.append("ARRAY_CONSTRUCT(DF1.FIRST_NAME, DF2.LAST_NAME) AS FIRST_LAST_NAME")
        pairing_columns = ', '.join(col_list)

        session.sql(f"""
            CREATE OR REPLACE TEMP VIEW {intermediate_db_name}.{intermediate_schema_name}.FILTERED_VIEW AS
            SELECT *
            FROM {intermediate_db_name}.{intermediate_schema_name}.{source_name}_DEDUPE_INPUT
            WHERE {pairing_field} != 'UNDEFINED'
              AND {pairing_field} NOT IN ('', {values_to_remove.upper()})
        """).collect()

        if method == 'BLOCK':
            session.sql(f"""
                SELECT
                    {pairing_columns},
                    '{pairing_field}' AS PAIRED_ON,
                    'BLOCK'           AS PAIRING_METHOD
                FROM {intermediate_db_name}.{intermediate_schema_name}.FILTERED_VIEW DF1
                INNER JOIN {intermediate_db_name}.{intermediate_schema_name}.FILTERED_VIEW DF2
                    ON  DF1.{pairing_field} = DF2.{pairing_field}
                WHERE DF1.SORT_INDEX > DF2.SORT_INDEX
            """).write.mode('append').save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_PAIRS_STAGE")

        else:
            window = int((window_size - 1) / 2)
            session.sql(f"""
                CREATE OR REPLACE TEMP VIEW {intermediate_db_name}.{intermediate_schema_name}.DATA_LEFT_VIEW AS
                WITH SNH_VIEW AS (
                    SELECT *,
                           {pairing_field}  AS SORTING_KEY,
                           ROW_NUMBER() OVER (ORDER BY {pairing_field}, PK) AS SORTING_INDEX
                    FROM {intermediate_db_name}.{intermediate_schema_name}.FILTERED_VIEW
                ),
                SORTING_KEY_FACTORS_VIEW AS (
                    SELECT DISTINCT SORTING_KEY,
                           ROW_NUMBER() OVER (ORDER BY SORTING_KEY) AS RN
                    FROM SNH_VIEW
                )
                SELECT DF1.* EXCLUDE (SORTING_INDEX, SORTING_KEY),
                       DF2.RN            AS LEFT_SORTING_KEY,
                       DF1.SORTING_INDEX AS LEFT_SORTING_INDEX
                FROM SNH_VIEW DF1
                INNER JOIN SORTING_KEY_FACTORS_VIEW DF2
                    ON DF1.SORTING_KEY = DF2.SORTING_KEY
            """).collect()

            for i in range(-window, window + 1):
                session.sql(f"""
                    WITH DATA_RIGHT_VIEW AS (
                        SELECT
                            * EXCLUDE (LEFT_SORTING_KEY, LEFT_SORTING_INDEX),
                            LEFT_SORTING_KEY   + {i} AS RIGHT_SORTING_KEY,
                            LEFT_SORTING_INDEX        AS RIGHT_SORTING_INDEX
                        FROM {intermediate_db_name}.{intermediate_schema_name}.DATA_LEFT_VIEW
                    )
                    SELECT
                        {pairing_columns},
                        '{pairing_field}' AS PAIRED_ON,
                        'SNH'             AS PAIRING_METHOD
                    FROM {intermediate_db_name}.{intermediate_schema_name}.DATA_LEFT_VIEW  DF1
                    INNER JOIN DATA_RIGHT_VIEW DF2
                        ON  DF1.LEFT_SORTING_KEY = DF2.RIGHT_SORTING_KEY
                    WHERE LEFT_SORTING_INDEX > RIGHT_SORTING_INDEX
                """).write.mode('append').save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_PAIRS_STAGE")

    all_stage_cols = [field.name for field in stage_schema.fields]
    partition_cols = [c for c in all_stage_cols if c not in ('PK', 'SORT_INDEX', 'PAIRED_ON', 'PAIRING_METHOD')]
    partition_cs = ', '.join(partition_cols)

    pairs_df = session.sql(f"""
        SELECT
            * EXCLUDE (SORT_INDEX),
            CURRENT_TIMESTAMP() AS PAIRS_INSERTED_DATE,
            CURRENT_USER()      AS PAIRS_INSERTED_BY
        FROM {intermediate_db_name}.{intermediate_schema_name}.{source_name}_PAIRS_STAGE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {partition_cs}
            ORDER BY SORT_INDEX
        ) = 1
    """)

    pairs_df.filter('PK[0] != PK[1]') \
            .write.mode('overwrite') \
            .save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_PAIRS")


def applying_rules(session, intermediate_db_name, intermediate_schema_name, source_name, similarity_algo_fields, duplicate_filtering_rules):
    similarity_algo_fields = {k: v.lower() for k, v in similarity_algo_fields.items()}

    select_clause = []
    for field, algorithm in similarity_algo_fields.items():
        if algorithm == 'jarowinkler':
            select_clause.append(f"(JAROWINKLER_SIMILARITY({field}[0], {field}[1])) / 100 AS {field}")
        else:
            select_clause.append(
                f"1 - (EDITDISTANCE({field}[0], {field}[1]) / "
                f"IFF(GREATEST(LENGTH({field}[0]), LENGTH({field}[1])) = 0, 1, "
                f"GREATEST(LENGTH({field}[0]), LENGTH({field}[1])))) AS {field}"
            )
    select_cs = ', '.join(select_clause)

    pairs_df = session.sql(f"""
        SELECT
            PK, ID, {select_cs},
            CURRENT_TIMESTAMP() AS SCORE_INSERTED_DATE,
            CURRENT_USER()      AS SCORE_INSERTED_BY
        FROM {intermediate_db_name}.{intermediate_schema_name}.{source_name}_PAIRS
    """)

    pairs_df.write.mode('overwrite').save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_SIMILARITY_SCORE")

    pairs_df = pairs_df.with_column('RULE_ID', lit('0'))
    duplicates = session.create_dataframe([], schema=pairs_df.schema)

    for rule_id, rule_expr in duplicate_filtering_rules.items():
        pairs_df = pairs_df.with_column('RULE_ID', when(expr(rule_expr), lit(rule_id)).otherwise(col('RULE_ID')))
        duplicates = duplicates.union_all(pairs_df.filter(col('RULE_ID') == rule_id))
        pairs_df = pairs_df.filter(col('RULE_ID') == '0')

    duplicates = duplicates.with_column('DUPLICATES_INSERTED_DATE', current_timestamp()).with_column('DUPLICATES_INSERTED_BY', current_user())
    duplicates.write.mode('overwrite').save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_DUPLICATES")


def clustering(session, intermediate_db_name, intermediate_schema_name, source_name, output_query):
    grouped_schema = StructType([StructField('PK_ARRAY', ArrayType(VariantType()))])
    session.create_dataframe([], schema=grouped_schema).write.mode('overwrite').save_as_table(f"{intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK")

    session.sql(f"""
        DECLARE
            PK_CNT INT;
        BEGIN
            INSERT OVERWRITE INTO {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK
            WITH GROUPED_BY_MIN AS (
                SELECT ARRAY_DISTINCT(ARRAY_UNION_AGG(PK)) AS PK
                FROM   {intermediate_db_name}.{intermediate_schema_name}.{source_name}_DUPLICATES
                GROUP  BY ARRAY_MIN(PK)
            ),
            GROUPED_BY_MAX AS (
                SELECT ARRAY_DISTINCT(ARRAY_UNION_AGG(PK)) AS PK
                FROM   GROUPED_BY_MIN
                GROUP  BY ARRAY_MAX(PK)
            )
            SELECT PK AS PK_ARRAY FROM GROUPED_BY_MAX;

            PK_CNT := (
                WITH CTE AS (
                    SELECT COUNT(VALUE) AS CNT
                    FROM   {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK,
                           LATERAL FLATTEN(INPUT => PK_ARRAY)
                    GROUP  BY VALUE
                )
                SELECT NVL(MAX(CNT), 0) FROM CTE
            );

            WHILE (:PK_CNT > 1) DO
                INSERT OVERWRITE INTO {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK
                WITH OVERLAPPED AS (
                    SELECT ARRAY_DISTINCT(
                        ARRAY_CAT(T1.PK_ARRAY, NVL(T2.PK_ARRAY, ARRAY_CONSTRUCT()))
                    ) AS PK_ARRAY
                    FROM   {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK T1
                    LEFT JOIN {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK T2
                        ON  ARRAYS_OVERLAP(T1.PK_ARRAY, T2.PK_ARRAY)
                        AND T1.PK_ARRAY <> T2.PK_ARRAY
                )
                SELECT ARRAY_DISTINCT(ARRAY_UNION_AGG(PK_ARRAY)) AS PK_ARRAY
                FROM   OVERLAPPED
                GROUP  BY ARRAY_MIN(PK_ARRAY);

                PK_CNT := (
                    WITH CTE AS (
                        SELECT COUNT(VALUE) AS CNT
                        FROM   {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK,
                               LATERAL FLATTEN(INPUT => PK_ARRAY)
                        GROUP  BY VALUE
                    )
                    SELECT NVL(MAX(CNT), 0) FROM CTE
                );
            END WHILE;
        END
    """).collect()

    final_cluster_df = session.sql(f"""
        WITH GET_CLUSTER_ID AS (
            WITH CTE AS (
                SELECT PK_ARRAY, ROW_NUMBER() OVER (ORDER BY PK_ARRAY) AS CLUSTER_ID
                FROM   {intermediate_db_name}.{intermediate_schema_name}.{source_name}_GROUPED_PK
            )
            SELECT TRIM(VALUE) AS GROUP_PK, CLUSTER_ID FROM CTE, LATERAL FLATTEN(INPUT => PK_ARRAY)
        ),
        GET_RULE_ID AS (
            SELECT TRIM(VALUE) AS DUPS_PK, RULE_ID FROM {intermediate_db_name}.{intermediate_schema_name}.{source_name}_DUPLICATES, LATERAL FLATTEN(INPUT => PK)
        ),
        OUTPUT_DATA AS (
            SELECT INP.PK, OUT.* FROM ({output_query}) OUT
            LEFT JOIN {intermediate_db_name}.{intermediate_schema_name}.{source_name}_DEDUPE_INPUT INP ON OUT.ID = INP.ID
        )
        SELECT
            OUTPUT_DATA.* EXCLUDE (PK), CID.CLUSTER_ID, LISTAGG(DISTINCT RID.RULE_ID, ',') AS RULE_ID, CURRENT_TIMESTAMP() AS DEDUPE_INSERTED_DATE
        FROM   OUTPUT_DATA
        LEFT JOIN GET_CLUSTER_ID CID ON OUTPUT_DATA.PK = CID.GROUP_PK
        LEFT JOIN GET_RULE_ID    RID ON OUTPUT_DATA.PK = RID.DUPS_PK
        GROUP BY ALL
    """)
    return final_cluster_df


def model(dbt, session):
    dbt.config(
        materialized="table",
        packages=["snowflake-snowpark-python==1.28.0"]
    )

    config_source_name = "HCHB_EMPLOYEE"

    config_rows = session.sql(f"""
        SELECT INPUT_QUERY, OUTPUT_QUERY, CONFIG, RULE_TYPE, INTERMEDIATE_DATABASE_NAME, INTERMEDIATE_SCHEMA_NAME
        FROM DATA_INGESTION_SOLUTION.ETL_MANAGEMENT.ETL_CONFIG_DEDUPE_DETAILS_SNOWPARK
        WHERE UPPER(SOURCE_NAME) = '{config_source_name}'
    """).collect()

    if not config_rows:
        raise ValueError(f"No active registration records found for '{config_source_name}'.")

    row = config_rows[0]
    config_dict = json.loads(row['CONFIG'])
    rule_type = row['RULE_TYPE']

    rules_rows = session.sql(f"SELECT RULES FROM DATA_INGESTION_SOLUTION.ETL_MANAGEMENT.ETL_CONFIG_DEDUPE_RULES_SNOWPARK WHERE RULE_TYPE = '{rule_type}'").collect()
    rules_dict = json.loads(rules_rows[0]['RULES'])

    pairing(
        session=session,
        intermediate_db_name=row['INTERMEDIATE_DATABASE_NAME'],
        intermediate_schema_name=row['INTERMEDIATE_SCHEMA_NAME'],
        source_name=config_source_name,
        input_query=row['INPUT_QUERY'],
        pairing_on=config_dict['PAIRS_ON'],
        remove_values=config_dict['REMOVE_VALUES'],
        window_size=3
    )

    applying_rules(
        session=session,
        intermediate_db_name=row['INTERMEDIATE_DATABASE_NAME'],
        intermediate_schema_name=row['INTERMEDIATE_SCHEMA_NAME'],
        source_name=config_source_name,
        similarity_algo_fields=config_dict['FIELDS'],
        duplicate_filtering_rules=rules_dict
    )

    final_output_df = clustering(
        session=session,
        intermediate_db_name=row['INTERMEDIATE_DATABASE_NAME'],
        intermediate_schema_name=row['INTERMEDIATE_SCHEMA_NAME'],
        source_name=config_source_name,
        output_query=row['OUTPUT_QUERY']
    )

    return final_output_df