from ksql import KSQLAPI
client = KSQLAPI('http://34.142.63.76:8088')

# First query create a stream and map to our topic
client.ksql("""CREATE STREAM application_records
  (
 ID VARCHAR,
 CODE_GENDER VARCHAR,
 FLAG_OWN_CAR VARCHAR,
 FLAG_OWN_REALTY VARCHAR,
 CNT_CHILDREN VARCHAR,
 AMT_INCOME_TOTAL VARCHAR,
 NAME_INCOME_TYPE VARCHAR,
 NAME_EDUCATION_TYPE VARCHAR,
 NAME_FAMILY_STATUS VARCHAR,
 NAME_HOUSING_TYPE VARCHAR,
 DAYS_BIRTH VARCHAR,
 DAYS_EMPLOYED VARCHAR,
 FLAG_MOBIL VARCHAR,
 FLAG_WORK_PHONE VARCHAR,
 FLAG_PHONE VARCHAR,
 FLAG_EMAIL VARCHAR,
 OCCUPATION_TYPE VARCHAR,
 CNT_FAM_MEMBERS VARCHAR
 )
 WITH (KAFKA_TOPIC='kafka-demo',
 VALUE_FORMAT='JSON');""")

# Creating a 2nd Stream as Select statement from the 1st one and adding casted field key
client.ksql("""CREATE STREAM deduplicated_app_records AS SELECT 
 CAST(ID AS STRING) + '/' + CAST(CNT_FAM_MEMBERS AS STRING) + '/' + CAST(CODE_GENDER AS STRING) + '/' + CAST(DAYS_BIRTH AS STRING) AS KEY,
 ID,
 CODE_GENDER,
 FLAG_OWN_CAR ,
 FLAG_OWN_REALTY,
 CNT_CHILDREN,
 AMT_INCOME_TOTAL,
 NAME_INCOME_TYPE,
 NAME_EDUCATION_TYPE,
 NAME_FAMILY_STATUS,
 NAME_HOUSING_TYPE,
 DAYS_BIRTH,
 DAYS_EMPLOYED,
 FLAG_MOBIL,
 FLAG_WORK_PHONE,
 FLAG_PHONE,
 FLAG_EMAIL,
 OCCUPATION_TYPE,
 CNT_FAM_MEMBERS
 FROM application_records;""")

# Creating 3rd Stream as Select Statement of 2nd one and making Key field to become key of the message
client.ksql("""CREATE STREAM deduplicated_app_records_wkey AS SELECT *
 FROM deduplicated_app_records
 PARTITION BY KEY;""")
