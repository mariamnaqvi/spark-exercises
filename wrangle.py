from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()


case = spark.read.csv("case.csv", header=True, inferSchema=True)
dept = spark.read.csv("dept.csv", header=True, inferSchema=True)
source = spark.read.csv("source.csv", header=True, inferSchema=True)



def wrangle_311(case, dept, source):

    case = prep_case(case)

    df = (
            case
            # do a left join keeping everything in the case df
            .join(dept, "dept_division", "left")
            # drop deptartment columns we don't need
            .drop(dept.dept_division)
            .drop(dept.dept_name)
            .drop(case.dept_division)
            .withColumnRenamed("standardized_dept_name", "department")
            # convert to a boolean
            .withColumn("dept_subject_to_SLA", col("dept_subject_to_SLA") == "YES")
        )
    df = df.join(source, on = df.source_id== source.source_id).drop(source.source_id)
    
    return df

def prep_case(case):
    '''
    Function preps the case df for joining
    '''
    case = case.withColumnRenamed('SLA_due_date', 'case_due_date')

    case = case.withColumn('case_closed', expr('case_closed == "YES"'))\
    .withColumn('case_late', expr('case_late == "YES"'))

    case = case.withColumn('council_district', col('council_district').cast('string'))

    # set up date format 
    fmt = "M/d/yy H:mm"

    case = case.withColumn('case_opened_date', to_timestamp('case_opened_date', fmt))\
    .withColumn('case_closed_date', to_timestamp('case_closed_date', fmt))\
    .withColumn('case_due_date', to_timestamp('case_due_date', fmt))

    case = case.withColumn('request_address', trim(lower(case.request_address)))

    case = case.withColumn('zipcode', regexp_extract('request_address', r"(\d+$)", 1))

    case = (
        case.withColumn(
            "case_age", datediff(current_timestamp(), "case_opened_date")
        )
        .withColumn(
            "days_to_closed", datediff("case_closed_date", "case_opened_date")
        )
        .withColumn(
            "case_lifetime",
            when(expr("! case_closed"), col("case_age")).otherwise(
                col("days_to_closed")
            ),
        )
    )

    case = case.withColumn('council_district', format_string('%03d', col('council_district').cast('int')))

    return case