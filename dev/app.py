'''--- How to use the script ---
1) Login to Snowflake using the web browser (any account)
2) Open new Python Worksheet
3) Set the Worksheet parameters: Settings -> ReturnType -> String
4) Run script. After successful execution te script generates 1 output row.
5) Copy returned row. NOTE: When copying data directly from the row you can lose new line characters. Copy script using "Copy button".
'''

import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import random

global_session = ''

def generatePassword(maxLen = 15):
    DIGITS            = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'] 
    LOCASE_CHARACTERS = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    UPCASE_CHARACTERS = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z']
    SYMBOLS           = ['#', '$', '%', '=', ':', '?', '.', '|', '~', '!', '^', '*', '(', ')', '-', '_', '+', '<', ',', '>', ';', '{', '}', '[', ']']
    password          = ''
    rNum              = 0
    n0 = n1 = n2 = n3 = True
    while len(password) < maxLen:
        rNum = random.choice([0, 1, 2, 3])
        if rNum == 0:
            password += random.choice(UPCASE_CHARACTERS)
            n0        = False
        elif rNum == 1:
            password += random.choice(DIGITS)
            n1        = False
        elif rNum == 2:
            password += random.choice(LOCASE_CHARACTERS)
            n2        = False
        elif rNum == 3:
            password += random.choice(SYMBOLS)
            n3        = False
        if len(password) == 7:
            if n0:
                password += random.choice(UPCASE_CHARACTERS)
            if n1:
                password += random.choice(DIGITS)
            if n2:
                password += random.choice(LOCASE_CHARACTERS)
            if n3:
                password += random.choice(SYMBOLS)
    return(password)

SRC_CTE = """
WITH 
SRC AS (
    SELECT
         TRIM(category  ) category
        ,TRIM(key       ) key
        ,TRIM(value     ) value
        ,TRIM(parameter1) parameter1
        ,TRIM(parameter2) parameter2
        ,TRIM(parameter3) parameter3
        ,TRIM(parameter4) parameter4
        ,TRIM(parameter5) parameter5
        ,TRIM(parameter6) parameter6
    FROM VALUES 
               ('General', 'Domain Name', 'IO_FIT', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Domain Code', 'IO_FIT', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Account Region', 'EU', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Domain Region', 'EU', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Warehouse Prefix', 'WH_EU', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Service User Prefix', 'SVC_EU', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Role Prefix', 'NN_EU', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Role Owner', 'SECURITYADMIN', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Role Grant', 'SYSADMIN', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'DB Owner', 'DOMAIN_ADMIN', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Schema Owner', 'DOMAIN_ADMIN', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'Public Schema Owner', 'ACCOUNTADMIN', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'WH TYPE', '''STANDARD''', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'WH AUTO_SUSPEND', '60', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'WH AUTO_RESUME', 'True', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'WH MIN_CLUSTER_COUNT', '1', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'WH MAX_CLUSTER_COUNT', '2', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('General', 'WH SCALING_POLICY', '''STANDARD''', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Environments', 'Development', 'DEV', 'NPROD', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Environments', 'Test / UAT', 'TEST', 'NPROD', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Environments', 'Production', 'PROD', 'PROD', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Databases', 'IO_FIT', 'IO_FIT', '{ENV}', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Schemas', 'Staging', 'STAGE', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Schemas', 'Core', 'CORE', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Schemas', 'Reporting/Final', 'RPT', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Roles', 'Domain Admin', 'DOMAIN_ADMIN', 'n.a.', 'n.a.', 'AAD_PROVISIONER', '', 'n.a.', 'n.a.')
              ,('Roles', 'ETL', 'ETL', '{ENV}', 'RW,OL', '', '', '1', 'ETL')
              ,('Roles', 'BI', 'BI', '{ENV}', 'RO', '', '', '1', 'BI')
              ,('Roles', 'Developer', 'DEVELOPER', 'NPROD', 'RW,OL', 'AAD_PROVISIONER', '', '1', 'DEVELOPMENT')
              ,('Roles', 'Operator', 'OPERATOR', 'PROD', 'RW,OL', 'AAD_PROVISIONER', '', '1', 'OPERATIONS')
              ,('Roles', 'Reader', 'READER', '{ENV_PNP}', 'RO', 'AAD_PROVISIONER', '', '1', 'READERS')
              ,('Warehouse Bundles', '1', 'XS', '', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Service Users', 'ETL Service User', 'ETL', 'ETL', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')
              ,('Service Users', 'BI Service User', 'BI', 'BI', 'n.a.', 'n.a.', 'n.a.', 'n.a.', 'n.a.')

              AS SRC(category, key, value, parameter1, parameter2, parameter3, parameter4, parameter5, parameter6)
    WHERE SRC.key <> ''
),
PAR AS (
    SELECT "'Domain Name'" domain_name, "'Domain Code'" domain_code, "'Account Region'" account_region, "'Domain Region'" domain_region, "'Warehouse Prefix'" warehouse_prefix, "'Service User Prefix'" service_user_prefix, "'Role Prefix'" role_prefix, "'Role Owner'" role_owner, "'Role Grant'" role_grant, "'DB Owner'" db_owner, "'Schema Owner'" schema_owner, "'Public Schema Owner'" public_schema_owner, "'WH TYPE'" wh_type, "'WH AUTO_SUSPEND'" wh_auto_suspend, "'WH AUTO_RESUME'" wh_auto_resume, "'WH MIN_CLUSTER_COUNT'" wh_min_cluster_count, "'WH MAX_CLUSTER_COUNT'" wh_max_cluster_count, "'WH SCALING_POLICY'" wh_scaling_policy
      FROM SRC
           PIVOT(MAX(value) FOR key IN ('Domain Name', 'Domain Code', 'Account Region', 'Domain Region', 'Warehouse Prefix', 'Service User Prefix', 'Role Prefix', 'Role Owner', 'Role Grant', 'DB Owner', 'Schema Owner', 'Public Schema Owner', 'WH TYPE', 'WH AUTO_SUSPEND', 'WH AUTO_RESUME', 'WH MIN_CLUSTER_COUNT', 'WH MAX_CLUSTER_COUNT', 'WH SCALING_POLICY'))
    WHERE SRC.category = 'General'

),
ENV AS (
    SELECT
         key              name
        ,TRIM(value)      code
        ,TRIM(parameter1) env_pnp
    FROM SRC
    WHERE SRC.category = 'Environments'
),
ENV_TMP AS (
    SELECT '{ENV}'     mapping, code   , env_pnp FROM ENV UNION
    SELECT code        mapping, code   , env_pnp FROM ENV UNION
    SELECT '{ENV_PNP}' mapping, env_pnp, env_pnp FROM ENV UNION
    SELECT env_pnp     mapping, env_pnp, env_pnp FROM ENV 
),
ROL_SRC AS (
    SELECT
         'Functional'       role_type
        ,SRC.key            role_name
        ,SRC.value          role_short_code
        ,SRC.parameter1     role_postfix
        ,SRC.parameter2     map_role_2_ar
        ,SRC.parameter5     wh_bundles
        ,SRC.parameter6     wh_short_code
        ,IFF(NVL(SRC.parameter3, '') = '', PAR.role_owner, SRC.parameter3) role_owner
        ,IFF(NVL(SRC.parameter4, '') = '', PAR.role_grant, SRC.parameter4) role_grant
    FROM SRC
         CROSS JOIN PAR
    WHERE SRC.category = 'Roles'
    UNION ALL
    SELECT SRC.*, '' wh_short_code, PAR.role_owner, PAR.role_grant
    FROM PAR
         CROSS JOIN 
         VALUES ('Access', 'Read Only'   , 'RO', '{ENV}', '' ,'')
               ,('Access', 'Read Write'  , 'RW', '{ENV}', '' ,'')
               ,('Access', 'Object Level', 'OL', '{ENV}', '' ,'')
               AS SRC(role_type, role_name, role_short_code, role_postfix, map_role_2_ar, wh_bundles)
),
ROL_SEQ AS (
    SELECT
         SRC.Role_Type
        ,SRC.role_name
        ,SRC.role_short_code
        ,SRC.role_postfix
        ,SRC.wh_short_code
        ,PAR.role_prefix || IFF(SRC.Role_Type = 'Functional', '_SNFK_', '_AR_') || SRC.role_short_code || '_' || PAR.Domain_Code || IFF(ENV.code IS NULL, '', '_'||ENV.code) role_code
        ,ENV.code env_code
        ,DECODE(env_code, 'DEV', 10, 'TEST', 20, 'SANDBOX', 30, 'PROD', 50, 100) env_code_seq
        ,ENV.env_pnp
        ,PAR.domain_name
        ,SRC.map_role_2_ar
        ,SRC.role_owner
        ,SRC.role_grant
        ,SRC.wh_bundles
        ,ROW_NUMBER() OVER(ORDER BY SRC.Role_Type DESC
                                   ,CASE WHEN SRC.role_name = 'Domain Admin' THEN 1
                                         WHEN SRC.role_name = 'ETL'          THEN 2
                                         WHEN SRC.role_name = 'BI'           THEN 3
                                         ELSE 3
                                    END     
                                   ,SRC.role_name
                                   ,env_pnp
                                   ,env_code_seq
                                   )seq
    FROM            ROL_SRC SRC
         LEFT  JOIN ENV_TMP ENV ON ENV.mapping = SRC.role_postfix
         CROSS JOIN PAR     PAR
),
ROL AS (
    SELECT
         CASE WHEN role_short_code = 'DOMAIN_ADMIN'
              THEN 'CREATE ROLE ' || role_code || ' COMMENT = ''Functional Domain Admin role for ' || domain_name || ' domain.'''
              WHEN role_type = 'Access'
              THEN 'CREATE ROLE ' || role_code || ' COMMENT = ''Access role with ' || DECODE(role_short_code, 'OL', 'Object Level', 'RW', 'Read-Write', 'RO', 'Read Only', '') ||  ' privileges on ' || env_code || ' environment for ' || domain_name || ' domain.'''
              WHEN role_type = 'Functional'
              THEN 'CREATE ROLE ' || role_code || ' COMMENT = ''Functional ' || role_name ||  ' role on ' || env_code || ' environment for ' || domain_name || ' domain.'''
              ELSE 'CREATE ROLE '|| role_code ||' COMMENT = '''''
         END create_sql
        ,*
        ,       row_number() OVER(PARTITION BY SRC.role_type ORDER BY seq                  ) role_type_seq
        ,DECODE(row_number() OVER(PARTITION BY SRC.role_name ORDER BY seq     ), 1, 1, NULL) role_name_is_first
        ,DECODE(row_number() OVER(PARTITION BY SRC.role_name ORDER BY seq DESC), 1, 1, NULL) role_name_is_last
    FROM ROL_SEQ SRC
    ORDER BY SRC.seq
),
ROL_WH_BUNDLE AS (
    SELECT
         role_code
        ,env_pnp
        ,wh_bundle
    FROM (  SELECT
                 ROL.*
                ,TRANSLATE(PAR.value, '=:()', '####') src_tok
                ,TRIM(STRTOK(src_tok, '#', 1)) tmp_tok1
                ,TRIM(STRTOK(src_tok, '#', 2)) tmp_tok2
                ,iff(tmp_tok2 IS NULL, tmp_tok2, tmp_tok1)wh_env_code
                ,iff(tmp_tok2 IS NULL, tmp_tok1, tmp_tok2)wh_bundle
            FROM ROL
                ,TABLE(split_to_table(UPPER(ROL.wh_bundles), ',')) AS PAR
            WHERE NVL(ROL.env_code  , '') <> ''
              AND NVL(ROL.wh_bundles, '') <> ''
         )
    WHERE (   env_code = wh_env_code 
           OR wh_env_code IS NULL
          )
    QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY role_code ORDER BY wh_env_code NULLS LAST)
),
WH_BUNDLE_PARAMS AS (
    SELECT
         wh_boundle
        ,value
        ,NVL(type             , wh_type             ) type1
        ,IFF(CONTAINS(type1, ''''), type1, '''' || type1 || '''') type
        ,NVL(auto_suspend     , wh_auto_suspend     ) auto_suspend
        ,NVL(auto_resume      , wh_auto_resume      ) auto_resume
        ,NVL(min_cluster_count, wh_min_cluster_count) min_cluster_count
        ,NVL(max_cluster_count, wh_max_cluster_count) max_cluster_count
        ,NVL(scaling_policy   , wh_scaling_policy   ) scaling_policy1
        ,IFF(CONTAINS(scaling_policy1, ''''), scaling_policy1, '''' || scaling_policy1 || '''') scaling_policy
    FROM (
            SELECT wh_boundle, value, "'TYPE'" TYPE, "'AUTO_SUSPEND'" AUTO_SUSPEND, "'AUTO_RESUME'" AUTO_RESUME, "'MIN_CLUSTER_COUNT'" MIN_CLUSTER_COUNT, "'MAX_CLUSTER_COUNT'" MAX_CLUSTER_COUNT, "'SCALING_POLICY'" SCALING_POLICY
            FROM (
                    SELECT
                         SRC.key   wh_boundle
                        ,SRC.value
                        ,TRIM(STRTOK(PAR.value, '=', 1)) param_key
                        ,TRIM(STRTOK(PAR.value, '=', 2)) param_val
                    FROM SRC
                        ,TABLE(split_to_table(UPPER(SRC.parameter1), ',')) AS PAR
                    WHERE category = 'Warehouse Bundles'
                 )
                 PIVOT(MAX(param_val) FOR param_key IN ('TYPE', 'AUTO_SUSPEND', 'AUTO_RESUME', 'MIN_CLUSTER_COUNT', 'MAX_CLUSTER_COUNT', 'SCALING_POLICY'))
         )
         CROSS JOIN PAR
),
WH_BUNDLE AS (
    SELECT
         wh_boundle
        ,value wh_size_code
        ,'''' || DECODE(value, 'XS'  , 'XSMALL'   , 'S'   , 'SMALL'   , 'M'   , 'MEDIUM'
                         , 'L'   , 'LARGE'    , 'XL'  , 'XLARGE'  , 'XXL' , 'XXLARGE'
                         , 'X2L' , 'XXLARGE'  , 'XXXL', 'XXXLARGE', 'X3L' , 'XXXLARGE'
                         , 'X4L' , 'X4LARGE'  , 'X5L' , 'X5LARGE' , 'X6L' , 'X6LARGE') || '''' size
        ,type
        ,auto_suspend
        ,auto_resume
        ,min_cluster_count
        ,max_cluster_count
        ,scaling_policy
    FROM WH_BUNDLE_PARAMS
),
ROL_WH AS (
    SELECT
         WHB.*
        ,ROL.role_short_code
        ,PAR.warehouse_prefix || '_' || BND.wh_size_code || '_' || ROL.wh_short_code || '_' || PAR.domain_code || '_' || ROL.env_pnp wh_code
        ,ROW_NUMBER() OVER(                                              ORDER BY ROL.role_short_code, ROL.env_pnp, ROL.env_code, wh_code     )seq
        ,ROW_NUMBER() OVER(PARTITION BY ROL.role_short_code              ORDER BY                      ROL.env_pnp, ROL.env_code, wh_code     )rol_seq
        ,ROW_NUMBER() OVER(PARTITION BY ROL.role_short_code, ROL.env_pnp ORDER BY                                   ROL.env_code, wh_code     )env_pnp_first
        ,ROW_NUMBER() OVER(PARTITION BY ROL.role_short_code, ROL.env_pnp ORDER BY                                   ROL.env_code DESC, wh_code DESC)env_pnp_last
        ,ROW_NUMBER() OVER(PARTITION BY wh_code       ORDER BY wh_code )wh_code_seq
        ,ROW_NUMBER() OVER(PARTITION BY ROL.role_code ORDER BY REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(wh_code, '_XS_', 1), '_S_', 2), '_M_', 3), '_L_', 4), '_XL_', 5), '_XXL_', 7)) role_min_wh
        ,'CREATE WAREHOUSE '||wh_code||' WITH WAREHOUSE_SIZE = ' || size || ' WAREHOUSE_TYPE = ' || TYPE || ' AUTO_SUSPEND = '||AUTO_SUSPEND || ' AUTO_RESUME = ' || AUTO_RESUME || ' MIN_CLUSTER_COUNT = ' || MIN_CLUSTER_COUNT || ' MAX_CLUSTER_COUNT = ' || MAX_CLUSTER_COUNT || ' SCALING_POLICY = ' || SCALING_POLICY || ';' sql

        ,IFF(SRC.parameter1 IS NOT NULL, 'USAGE         ', 'USAGE, MONITOR') privs
        ,'GRANT ' || privs || ' ON WAREHOUSE ' || wh_code || ' TO ' || ROL.role_code || ';'grant_sql
    FROM            ROL_WH_BUNDLE WHB
         INNER JOIN WH_BUNDLE     BND ON WHB.wh_bundle = BND.wh_boundle
         INNER JOIN ROL           ROL ON WHB.role_code = ROL.role_code
         LEFT JOIN  SRC           SRC ON TRIM(SRC.parameter1) = ROL.role_short_code AND SRC.category = 'Service Users'
         CROSS JOIN PAR           PAR
),
AR_GRANTS AS (
    SELECT 
         RA.role_code
        ,RA.role_name
        ,RA.env_code
        ,RF.role_code tgt_role
        ,DECODE(ROW_NUMBER() OVER(PARTITION BY RA.role_name ORDER BY RA.seq, RF.seq), 1, 1, NULL)role_name_is_first
        ,DECODE(ROW_NUMBER() OVER(PARTITION BY RA.role_name, RA.env_code ORDER BY RA.seq, RF.seq), 1, 1, NULL)env_is_first
    FROM            ROL RA
         INNER JOIN (
                     SELECT ROL.*, VAL.value
                     FROM ROL
                         ,LATERAL SPLIT_TO_TABLE(ROL.map_role_2_ar, ',')VAL
                    ) RF 
           ON RA.role_short_code = RF.value
          AND CASE WHEN RF.role_postfix = '{ENV}'     AND RA.env_code = RF.env_code THEN 1
                   WHEN RF.role_postfix = '{ENV_PNP}' AND RA.env_pnp  = RF.env_pnp  THEN 2
                   WHEN RF.role_postfix = RA.env_pnp                                THEN 3
              END > 0
    WHERE RA.role_type = 'Access'
    ORDER BY RA.seq, RF.seq
),
DB AS (
    SELECT
         SRC.key        db_name
        ,SRC.value      db_short_code
        ,db_short_code || IFF(PAR.account_region <> PAR.domain_region, '_'||PAR.domain_region, '') || '_'||ENV_Data.code db_code
        ,ENV_Data.name      env_name
        ,ENV_Data.code      env_code
        ,ENV_Data.env_pnp env_pnp
        ,NVL(ROL.role_code, PAR.db_owner)db_owner
        ,ROW_NUMBER() OVER(ORDER BY 
                                    DECODE(env_code, 'DEV', 10, 'TEST', 20, 'SANDBOX', 30, 'PROD', 50, 100)
                                   ,db_short_code
                                   ,db_code) seq
    FROM SRC
         CROSS JOIN PAR
         LEFT  JOIN ROL ON PAR.db_owner = ROL.role_short_code
         LEFT  JOIN ENV ENV_Split ON CONTAINS(SRC.parameter1, '{ENV}') OR SRC.parameter1 = ENV_Split.code
         LEFT  JOIN ENV ENV_Data  ON ENV_Split.code = ENV_Data.code
    WHERE SRC.category = 'Databases'
    ORDER BY db_name
),
SCH AS (
    SELECT
         SRC.key          schema_name
        ,SRC.value        schema_code
        ,NVL(ROL.role_code, PAR.schema_owner)schema_owner
        ,0                  is_schema_created
        ,ROW_NUMBER() OVER(ORDER BY schema_code) seq
    FROM            SRC
         CROSS JOIN PAR
         LEFT  JOIN ROL ON PAR.schema_owner = ROL.role_short_code
    WHERE SRC.category = 'Schemas'
    UNION ALL
    SELECT
         'PUBLIC'    schema_name
        ,schema_name schema_short_code
        ,NVL(ROL.role_short_code, PAR.public_schema_owner)public_schema_owner
        ,1           is_schema_created
        ,100 seq
    FROM            PAR
         LEFT  JOIN ROL ON PAR.public_schema_owner = ROL.role_short_code
),
DB_SCH AS (
    SELECT
         DB.db_name
        ,DB.db_short_code
        ,DB.db_code
        ,DB.env_name
        ,DB.env_code
        ,DB.env_pnp
        ,DB.db_owner
        ,SCH.schema_name
        ,SCH.schema_code
        ,SCH.schema_owner
        ,SCH.is_schema_created
        ,PAR.domain_name
        ,ROW_NUMBER() OVER(ORDER BY DB.seq, SCH.seq) seq
        ,ROW_NUMBER() OVER(PARTITION BY env_code ORDER BY DB.seq, SCH.seq) env_seq
        ,DECODE(env_seq, 1, 1) env_is_first
        ,ROW_NUMBER() OVER(PARTITION BY env_code, DB.db_short_code ORDER BY DB.seq, SCH.seq) db_seq
        ,DECODE(db_seq, 1, 1) db_is_first
        ,ROW_NUMBER() OVER(PARTITION BY env_code, DB.db_short_code ORDER BY DB.seq DESC, SCH.seq DESC) db_seq2
        ,DECODE(db_seq2, 1, 1) db_is_last
    FROM            DB
         CROSS JOIN SCH
         CROSS JOIN PAR
),
AR_GRANTS_OBJ AS (
    SELECT
         SRC.*
        ,'GRANT USAGE ON DATABASE ' || db_code || ' TO ROLE ' || role_code || ';' grant_db
        ,CASE WHEN role_short_code = 'RO' AND is_public_schema = 0 
              THEN 'GRANT USAGE                       ON SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'RW' AND is_public_schema = 0 
              THEN 'GRANT USAGE                       ON SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'OL' AND is_public_schema = 0 
              THEN 'GRANT USAGE, ALL                  ON SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
         END grant_schema
        ,CASE WHEN role_short_code = 'RO' AND is_public_schema = 0 
              THEN 'GRANT SELECT                      ON FUTURE TABLES IN SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'RW' AND is_public_schema = 0 
              THEN 'GRANT SELECT,INSERT,UPDATE,DELETE ON FUTURE TABLES IN SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'OL' AND is_public_schema = 0 
              THEN 'GRANT ALL                         ON FUTURE TABLES IN SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
         END grant_schema_future_tables
        ,CASE WHEN role_short_code = 'RO' AND is_public_schema = 0 
              THEN 'GRANT SELECT                      ON FUTURE VIEWS  IN SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'RW' AND is_public_schema = 0 
              THEN 'GRANT SELECT                      ON FUTURE VIEWS  IN SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'OL' AND is_public_schema = 0 
              THEN 'GRANT ALL                         ON FUTURE VIEWS  IN SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
         END grant_schema_future_views
        ,CASE WHEN role_short_code = 'RO' AND is_public_schema = 1
              THEN 'GRANT USAGE                       ON SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'RW' AND is_public_schema = 1
              THEN 'GRANT USAGE                       ON SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
              WHEN role_short_code = 'OL' AND is_public_schema = 1
              THEN 'GRANT USAGE, ALL                  ON SCHEMA ' || db_code || '.' || schema_code ||  ' TO ROLE ' || role_code || ';'
         END grant_schema_public
        ,ROW_NUMBER() OVER(PARTITION BY role_short_code                    ORDER BY seq     ) role_first
        ,ROW_NUMBER() OVER(PARTITION BY role_short_code, env_code          ORDER BY seq     ) role_env_first
        ,ROW_NUMBER() OVER(PARTITION BY role_short_code, env_code          ORDER BY seq DESC) role_env_last
        ,ROW_NUMBER() OVER(PARTITION BY role_short_code, env_code, db_code ORDER BY seq     ) role_env_db_first
        ,ROW_NUMBER() OVER(PARTITION BY role_short_code, env_code, db_code ORDER BY seq DESC) role_env_db_last
    FROM (
            SELECT
                 ROL.role_short_code
                ,ROL.env_code
                ,ROL.role_code
                ,DBS.db_code
                ,DBS.schema_code
                ,DECODE(DBS.schema_code, 'PUBLIC', 1, 0) is_public_schema
                ,ROW_NUMBER() OVER(ORDER BY ROL.role_short_code, ROL.env_code, DBS.db_code, is_public_schema DESC, DBS.schema_code) seq
            FROM            ROL    ROL
                 INNER JOIN DB_SCH DBS ON ROL.env_code = DBS.env_code
            WHERE ROL.role_type = 'Access'
         )SRC
     ORDER BY seq
),
SRV_USERS AS (
    SELECT
         SRC.key              name
        ,TRIM(SRC.value)      user_short_code
        ,TRIM(SRC.parameter1) user_role_short_code
        ,PAR.service_user_prefix || '_' || user_short_code || '_' || PAR.domain_code || '_' || ROL.env_code user_code
        ,ROL.env_code
        ,RPAD(user_code, MAX(LENGTH(user_code)) OVER(), ' ') final_user_name
        ,RPAD(ROL.role_code, MAX(LENGTH(ROL.role_code)) OVER(), ' ') final_role_code
        ,'CREATE USER ' || final_user_name || ' LOGIN_NAME = ' || final_user_name || ' DISPLAY_NAME = ' || final_user_name || ';' create_sql
        ,'ALTER  USER ' || final_user_name || ' SET DEFAULT_ROLE = ''' || ROL.role_code || ''', DEFAULT_NAMESPACE = ''' || DB.db_code || ''', DEFAULT_WAREHOUSE = ''' || ROL_WH.wh_code || ''';' alter_sql
        ,'GRANT ROLE '  || final_role_code || ' TO USER ' || user_code || ';' grant_sql
        ,ROW_NUMBER() OVER(ORDER BY SRC.key, user_code) seq
        ,ROW_NUMBER() OVER(PARTITION BY SRC.key ORDER BY SRC.key, user_code) seq_user
    FROM            SRC
         CROSS JOIN PAR
         INNER JOIN ROL    ON user_role_short_code = ROL.role_short_code
         LEFT  JOIN ROL_WH ON ROL.role_code = ROL_WH.role_code AND ROL_WH.role_min_wh=1
         LEFT  JOIN DB     ON DB.env_code   = ROL.env_code
    WHERE SRC.category = 'Service Users'
    QUALIFY 1 = ROW_NUMBER() OVER(PARTITION BY user_code ORDER BY DB.db_code)
)
"""

def createRoles():
    sqlText = SRC_CTE + '''
    SELECT * 
    FROM ROL 
    ORDER BY seq    
    '''
    rows = global_session.sql(sqlText).collect()
    scriptCreate       = ''
    scriptOwnerSSO     = ''
    scriptOwnerDefault = ''
    scriptGrant        = ''
    scriptGrantAr      = ''

    #Generate scripts with some logics related to Role creation
    for row in rows:
        roleType        = row["ROLE_TYPE"]
        roleName        = row["ROLE_NAME"]
        roleCode        = row["ROLE_CODE"]
        envCode         = row["ENV_CODE"]
        roleOwner       = row["ROLE_OWNER"]
        roleGrant       = row["ROLE_GRANT"]
        seq             = row["SEQ"]
        roleTypeSeq     = row["ROLE_TYPE_SEQ"]
        roleNameIsFirst = row["ROLE_NAME_IS_FIRST"]
        createSql       = row["CREATE_SQL"]

        if seq == 1:
            #Create role script: Header for the whole script
            scriptCreate += f'''
----------------------------------------------------------------------------------------------------------------
-- CREATE ROLES
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;'''
        if roleTypeSeq == 1:
            #Create role script: Header for role type Functional/Access
            scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
----  CREATE {roleType.upper()} ROLES
----------------------------------------------------------------------------------------------------------------'''
        if roleNameIsFirst == 1:
            #Create role script: Header for a given role ETL, Developer, etv
            scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
------  {roleName} role for Domain
----------------------------------------------------------------------------------------------------------------'''
        #Create role script: CREATE statement
        scriptCreate += f'''
-------- {envCode}
{createSql};'''

        #Grants for SSO - Header
        if seq == 1:
            scriptOwnerSSO += f'''

----------------------------------------------------------------------------------------------------------------
----  Grant OWNERSHIP required by SSO
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
'''
        if roleOwner == 'AAD_PROVISIONER':
            #Grants for SSO - GRANT statement
            scriptOwnerSSO += f'''
GRANT OWNERSHIP ON ROLE {roleCode} TO ROLE AAD_PROVISIONER WITH GRANT OPTION COPY CURRENT GRANTS;'''


        #Set owner of the role - Header
        if seq == 1:
            scriptOwnerDefault += f'''

----------------------------------------------------------------------------------------------------------------
----  Grant default OWNERSHIP
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
'''
        #Set owner of the role - GRANT statement
        if roleOwner != 'AAD_PROVISIONER':
            scriptOwnerDefault += f'''
GRANT OWNERSHIP ON ROLE {roleCode} TO ROLE {roleOwner} WITH GRANT OPTION COPY CURRENT GRANTS;'''

        #Default GRANT USAGE on created roles - Header
        if seq == 1:
            scriptGrant += f'''

----------------------------------------------------------------------------------------------------------------
----  Grant default USAGE on roles
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
'''
        if roleGrant != '':
            #Default GRANT USAGE on created roles - GRANT Statement
            scriptGrant += f'''
GRANT ROLE {roleCode} TO ROLE {roleGrant} ;'''


    #GRANT ACCESS ROLES TO ROLES
    sqlText = SRC_CTE + '''
    SELECT * 
    FROM AR_GRANTS  
    '''
    rows = global_session.sql(sqlText).collect()
    
    #GRANT ACCESS ROLES TO CREATED ROLES - Header
    scriptGrantAr += f'''
----------------------------------------------------------------------------------------------------------------
----  GRANT Access Roles to Roles
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;'''
    for row in rows:
        roleCode        = row["ROLE_CODE"]
        roleName        = row["ROLE_NAME"]
        envCode         = row["ENV_CODE"]
        tgtRole         = row["TGT_ROLE"]
        roleNameIsFirst = row["ROLE_NAME_IS_FIRST"]
        envIsFirst      = row["ENV_IS_FIRST"]

        
        #GRANT ACCESS ROLES TO CREATED ROLES - Header for a given role
        if roleNameIsFirst == 1:
            scriptGrantAr += f'''

----------------------------------------------------------------------------------------------------------------
------  Grant Access Role {roleName}
----------------------------------------------------------------------------------------------------------------'''

        #GRANT ACCESS ROLES TO CREATED ROLES - GRANT Statement
        if envIsFirst == 1:
            scriptGrantAr += f'''
-------- {envCode}'''

        scriptGrantAr += f'''
GRANT ROLE {roleCode} TO ROLE {tgtRole};'''

    result = scriptCreate + '\n' + scriptOwnerSSO + '\n' + scriptOwnerDefault + '\n' + scriptGrant + '\n' + scriptGrantAr
    return result;

def createDatabases():
    sqlText = SRC_CTE + '''
    SELECT *
    FROM DB_SCH
    ORDER BY seq'''
    rows = global_session.sql(sqlText).collect()
    scriptCreate    = ''
    scriptGrant     = ''
   
    #Generate scripts with some logics related to Role creation
    #Create DB script: Header for the whole script
    scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
-- CREATE DATABASES, SCHEMAS AND DEFINE OWNERSHIP
----------------------------------------------------------------------------------------------------------------
USE ROLE SYSADMIN;'''
    for row in rows:
        dbName          = row["DB_NAME"]
        dbShortCode     = row["DB_SHORT_CODE"]
        dbCode          = row["DB_CODE"]
        envCode         = row["ENV_CODE"]
        envName         = row["ENV_NAME"]
        envPnp          = row["ENV_PNP"]
        dbOwner         = row["DB_OWNER"]
        domainName      = row["DOMAIN_NAME"]
        schemaName      = row["SCHEMA_NAME"]
        schemaCode      = row["SCHEMA_CODE"]
        schemaOwner     = row["SCHEMA_OWNER"]
        isSchemaCreated = row["IS_SCHEMA_CREATED"]
        seq             = row["SEQ"]
        envSeq          = row["ENV_SEQ"]
        envIsFirst      = row["ENV_IS_FIRST"]
        dbSeq           = row["DB_SEQ"]
        dbIsFirst       = row["DB_IS_FIRST"]
        dbIsLast        = row["DB_IS_LAST"]
        
        if envIsFirst == 1:
            #Create DB script: Header for a given environment
            scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
---- Create objects for {envCode} environment
----------------------------------------------------------------------------------------------------------------'''
        if dbIsFirst == 1:
            #Create DB script: Header for a given database
            scriptCreate += f'''

---- {dbCode}
------ Create objects
CREATE DATABASE {dbCode} COMMENT = 'Database of {domainName} Data Domain on {envName} environment';'''
        if isSchemaCreated == 0:
            scriptCreate += f'''
CREATE SCHEMA {dbCode}.{schemaCode} WITH MANAGED ACCESS COMMENT = '{schemaName} schema in {dbName} database on {envCode} environment';'''

            scriptGrant += f'''
GRANT OWNERSHIP ON SCHEMA {dbCode}.{schemaCode} TO ROLE {schemaOwner};'''
        if dbIsLast == 1:
            scriptGrant += f'''
GRANT OWNERSHIP ON DATABASE {dbCode} TO ROLE {dbOwner};'''
            scriptCreate += '\n' + scriptGrant
            scriptGrant = '''------ Grant Ownership'''
    #print(scriptCreate)
    return scriptCreate



def createWH():
    sqlText = SRC_CTE + '''
    SELECT *
    FROM ROL_WH
    ORDER BY seq'''
    rows = global_session.sql(sqlText).collect()
    scriptCreate    = ''
    scriptGrant     = ''
   
    #Generate scripts with some logics related to Role creation
    #Create DB script: Header for the whole script
    scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
-- CREATE WAREHOUSES
----------------------------------------------------------------------------------------------------------------
USE ROLE SYSADMIN;
'''
    for row in rows:
        roleCode      = row["ROLE_CODE"]
        roleShortCode = row["ROLE_SHORT_CODE"]
        envPnp        = row["ENV_PNP"]
        whBundle      = row["WH_BUNDLE"]
        whCode        = row["WH_CODE"]
        seq           = row["SEQ"]
        rolSeq        = row["ROL_SEQ"]
        envPnpFirst   = row["ENV_PNP_FIRST"]
        sql           = row["SQL"]
        envPnpLast    = row["ENV_PNP_LAST"]
        whCodeSeq     = row["WH_CODE_SEQ"]
        grantSql      = row["GRANT_SQL"]

        if rolSeq == 1:
            scriptCreate += f"""
---- Create warehouses for users of role {roleShortCode}"""
        
        if envPnpFirst == 1:
            scriptCreate += f"""
------ {envPnp}"""
        
        if whCodeSeq == 1:
            scriptCreate += f"""
{sql}"""
        
        scriptGrant += f"""
{grantSql}"""
        
        if envPnpLast == 1:
            scriptCreate += scriptGrant + '\n'
            scriptGrant  = ''

    return scriptCreate



def createGrantsAR2Objects():
    sqlText = SRC_CTE + '''
SELECT *
FROM AR_GRANTS_OBJ
ORDER BY seq'''
    rows = global_session.sql(sqlText).collect()
    scriptCreate    = ''
    scriptGrant     = ''
    scriptGrantPubl = ''
   
    #Generate scripts to grant access to objects for Access Roles
    #Create DB script: Header for the whole script
    for row in rows:
        roleShortCode          = row["ROLE_SHORT_CODE"]
        envCode                = row["ENV_CODE"]
        roleCode               = row["ROLE_CODE"]
        dbCode                 = row["DB_CODE"]
        schemaCode             = row["SCHEMA_CODE"]
        isPublicSchema         = row["IS_PUBLIC_SCHEMA"]
        grantDb                = row["GRANT_DB"]
        grantSchema            = row["GRANT_SCHEMA"]
        grantSchemaFutureTables= row["GRANT_SCHEMA_FUTURE_TABLES"]
        grantSchemaFutureViews = row["GRANT_SCHEMA_FUTURE_VIEWS"]
        grantSchemaPublic      = row["GRANT_SCHEMA_PUBLIC"]
        seq                    = row["SEQ"]
        roleFirst              = row["ROLE_FIRST"]
        roleEnvFirst           = row["ROLE_ENV_FIRST"]
        roleEnvLast            = row["ROLE_ENV_LAST"]
        roleEnvDbFirst         = row["ROLE_ENV_DB_FIRST"]
        roleEnvDbLast          = row["ROLE_ENV_DB_LAST"]
        
        if seq == 1:
            scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
-- GRANT PRIVILEGES TO ACCESS ROLES
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;
'''
        if roleFirst == 1:
            scriptCreate += f'''
----------------------------------------------------------------------------------------------------------------
---- Grant for {roleShortCode} Access roles
----------------------------------------------------------------------------------------------------------------'''

        if roleEnvFirst == 1:
            scriptCreate += f'''
------ {envCode}'''
        if roleEnvDbFirst == 1:
            scriptCreate += f'''
-------- {dbCode}
{grantDb}'''

        if isPublicSchema == 0:
            scriptCreate += f'''
{grantSchema}'''
            scriptGrant += f'''
{grantSchemaFutureTables}
{grantSchemaFutureViews}'''
        if isPublicSchema == 1:
            scriptGrantPubl += f'''
{grantSchemaPublic}
'''
        if roleEnvDbLast == 1:
            scriptCreate += f'''
{scriptGrant}
{scriptGrantPubl}'''

            scriptGrant = ''
            scriptGrantPubl = ''
    return scriptCreate



def createServiceUsers():
    sqlText = SRC_CTE + '''
SELECT *
FROM SRV_USERS
ORDER BY seq'''
    rows = global_session.sql(sqlText).collect()
    scriptCreate    = ''
    scriptAlter     = ''
    scriptGrant     = ''
   
    #Generate scripts to grant access to objects for Access Roles
    #Create DB script: Header for the whole script
    for row in rows:
        createSql         = row["CREATE_SQL"]
        grantSql          = row["GRANT_SQL"]
        alterSql          = row["ALTER_SQL"]
        seq               = row["SEQ"]
        seqUser           = row["SEQ_USER"]
        envCode           = row["ENV_CODE"]
        userRoleShortCode = row["USER_ROLE_SHORT_CODE"]
        
        if seq == 1:
            scriptCreate += f'''

----------------------------------------------------------------------------------------------------------------
-- CREATE SERVICE USERS ASD SET ROLES
----------------------------------------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

----------------------------------------------------------------------------------------------------------------
---- Create service account users
----------------------------------------------------------------------------------------------------------------'''

            scriptGrant += f'''

----------------------------------------------------------------------------------------------------------------
---- Grant roles service account users
----------------------------------------------------------------------------------------------------------------'''
            scriptAlter += f'''
'''

        if seqUser == 1:
            scriptCreate += f'''
------ Create {userRoleShortCode} service account user'''

            scriptGrant += f'''
------ Grant roles to {userRoleShortCode} service account user'''
            scriptAlter += f'''
'''

        scriptCreate += f'''
------ {envCode}
{createSql}'''

        scriptGrant += f'''
------ {envCode}
{grantSql}'''
        scriptAlter += f'''
------ {envCode}
{alterSql}'''

    scriptCreate += scriptAlter + scriptGrant
    return scriptCreate

        
def main(session: snowpark.Session): 
    global global_session
    global_session = session
    import snowflake.connector
    
    result = createRoles() + '\n' + createDatabases() + '\n' + createWH() + '\n' + createGrantsAR2Objects() + '\n' + createServiceUsers()
    def create_snowflake_worksheet(result):
    # Snowflake connection details
        conn = snowflake.connector.connect(
            user="SFTRAINING",
            password="Snowflake24!",
            account="axivxno-bwb79529",
            warehouse="COMPUTE_WH",
            database="GIT_INT",
            schema="DEMO"
        )
        
        sql_script = result
        create_worksheet_sql = f"""
    INSERT INTO GIT_INT.DEMO.SCRIPT_STORE (script)
    VALUES ('Generated Worksheet', $$ {sql_script} $$);
    """
        with conn.cursor() as cur:
            cur.execute(create_worksheet_sql)
            print("Worksheet created successfully.")
            conn.close()
            
    create_snowflake_worksheet(result)
    

    #return result
