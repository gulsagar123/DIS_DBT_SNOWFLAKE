import os
import snowflake.connector

print("Connecting to Snowflake to fetch metadata and cleansing rules...")
conn = snowflake.connector.connect(
    account="tkqgmnn-ydb88696",
    user="RAJATSAPKAL",
    password="@Rajatsapk1234",
    role="ACCOUNTADMIN",
    warehouse="COMPUTE_WH",
    database="DATA_INGESTION_SOLUTION",
    schema="SILVER_STAGING"
)

cursor = conn.cursor()

# 1. Fetch all active cleansing rules into Python memory
cursor.execute("""
    SELECT TRIM(UPPER(RULE_NAME)), TRIM(UPPER(MODE)), TRIM(UPPER(COLUMN_LIKE)), TRIM(UPPER(TYPE_FAMILY)), EXPR_TEMPLATE
    FROM DATA_INGESTION_SOLUTION.ETL_MANAGEMENT.CLEANSING_RULES
    WHERE IS_ACTIVE = TRUE
    ORDER BY APPLY_ORDER, RULE_NAME;
""")
rules = cursor.fetchall()

# 2. Fetch all active pipelines configuration records
cursor.execute("""
    SELECT 
        TRIM(PIPELINE_ID), TRIM(SRC_DB), TRIM(SRC_SCHEMA), TRIM(SRC_TABLE),
        TRIM(TGT_DB), TRIM(TGT_SCHEMA), TRIM(TGT_TABLE), TRIM(PK_COLS),
        TRIM(MODE), TRIM(LOAD_TYPE), TRIM(WATERMARK_COL), COALESCE(WATERMARK_LAG_MINS, 0)
    FROM DATA_INGESTION_SOLUTION.ETL_MANAGEMENT.DQ_PIPELINE_CONFIG
    WHERE IS_ACTIVE = TRUE;
""")
pipelines = cursor.fetchall()

output_dir = "models/silver"
os.makedirs(output_dir, exist_ok=True)

print(f"Starting compilation for {len(pipelines)} target models...")

for row in pipelines:
    pid, src_db, src_schema, src_table, tgt_db, tgt_schema, tgt_table, pk_cols, p_mode, load_type, wm_col, wm_lag = row
    
    # 3. For each table, query its exact column names and types from Snowflake
    col_query = f"""
        SELECT TRIM(UPPER(COLUMN_NAME)) as COLUMN_NAME, TRIM(UPPER(DATA_TYPE)) as DATA_TYPE
        FROM {src_db}.INFORMATION_SCHEMA.COLUMNS
        WHERE UPPER(TABLE_CATALOG) = UPPER('{src_db}')
          AND UPPER(TABLE_SCHEMA) = UPPER('{src_schema}')
          AND UPPER(TABLE_NAME) = UPPER('{src_table}')
        ORDER BY ORDINAL_POSITION;
    """
    cursor.execute(col_query)
    columns = cursor.fetchall()
    
    select_expressions = []
    
    for col in columns:
        col_name, col_type = col
        
        # Classify structural data type families
        type_fam = 'ANY'
        if 'TIMESTAMP' in col_type: type_fam = 'TIMESTAMP'
        elif 'DATE' in col_type: type_fam = 'DATE'
        elif any(x in col_type for x in ['NUMBER', 'INT', 'DECIMAL', 'FLOAT', 'DOUBLE']): type_fam = 'NUMBER'
        elif any(x in col_type for x in ['CHAR', 'TEXT', 'STRING', 'VARCHAR', 'VARIANT']): type_fam = 'TEXT'
        
        current_expr = f'"{col_name}"'
        ts_parsed = False
        
        # Evaluate matches against rules in Python memory
        for rule in rules:
            r_name, r_mode, r_like, r_family, expr_template = rule
            
            mode_allowed = (p_mode.upper() in ('ALL', '', 'NONE') or r_mode == p_mode.upper())
            family_matched = (r_family == 'ANY' or type_fam == 'ANY' or r_family == type_fam)
            
            # Safe substring rule matching
            clean_pattern = r_like.replace('%', '')
            like_matched = (r_like == '%') or (clean_pattern in col_name)
            
            if mode_allowed and family_matched and like_matched:
                if not (ts_parsed and r_name.startswith('DATE_PARSE')):
                    current_expr = expr_template.replace('{{COL}}', current_expr)
                    if r_name.startswith('TS_PARSE'):
                        ts_parsed = True
                        
        select_expressions.append(f'    {current_expr} AS "{col_name}"')
        
    select_clause = ",\n".join(select_expressions)
    
    # 4. Construct complete independent dbt SQL template file string
    has_watermark = wm_col and str(wm_col).strip().lower() not in ('none', '')
    
    dbt_code = f"""{{# Automated Python Compilation for Pipeline: {pid} #}}
{{{{
    config(
        materialized='incremental',
        unique_key='{pk_cols}',
        incremental_strategy='merge',
        alias='{tgt_table}'
    )
}}}}

SELECT
{select_clause}

FROM {src_db}.{src_schema}.{src_table}
"""

    if load_type == 'INCREMENTAL' and has_watermark:
        dbt_code += f"""
{{% if is_incremental() %}}
  WHERE "{wm_col}" > DATEADD(minute, -{wm_lag}, (SELECT MAX("{wm_col}") FROM {{{{ this }}}}))
{{% endif %}}
"""

    filename = f"{tgt_table.lower()}.sql"
    filepath = os.path.join(output_dir, filename)
    with open(filepath, "w") as f:
        f.write(dbt_code)
        
    print(f" -> Successfully compiled clean model file: {filepath}")

cursor.close()
conn.close()
print("\nAll pipeline models successfully generated with clean physical expressions built-in!")