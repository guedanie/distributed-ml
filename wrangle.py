from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def read_csv():
    spark = SparkSession.builder.getOrCreate()
    df_source = spark.read.csv("source.csv", sep = ',', header=True, inferSchema=True)
    df = spark.read.csv("case.csv", sep = ',', header=True, inferSchema=True)
    dept = spark.read.csv("dept.csv", sep = ',', header=True, inferSchema=True)

    return df_source, df, dept

def turn_values_to_bools(df):
    df = df.withColumn("case_closed", expr('case_closed == "YES"')).withColumn("case_late", expr('case_late == "YES"'))
    return df

def turn_district_string(df):
    df = df.withColumn("council_district", col("council_district").cast("string"))

    return df

def edit_address(df):
    df = df.withColumn("request_address", trim(lower(df.request_address)))
    return df

def create_days_late_to_weeks(df):
    df = df.withColumn("num_weeks_late", expr("num_days_late / 7 AS num_weeks_late"))
    return df

def change_to_date(df):
    # change dates to timestamp
    fmt = "M/d/yy H:mm"
    df = (
        df.withColumn("case_opened_date", to_timestamp("case_opened_date", fmt))
        .withColumn("case_closed_date", to_timestamp("case_closed_date", fmt))
        .withColumn("SLA_due_date", to_timestamp("SLA_due_date", fmt))
    )
    return df

def create_case_age(df):
    df = (
        df.withColumn(
            "case_age", datediff(to_date(lit("2018-08-08")), "case_opened_date")
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

    return df

def join_dept_data(df, dept):
    df = (
        df
        # left join on dept_division
        .join(dept, "dept_division", "left")
        # drop all the columns except for standardized name, as it has much fewer unique values
        .drop(dept.dept_division)
        .drop(dept.dept_name)
        .withColumnRenamed("standardized_dept_name", "department")
        # convert to a boolean
        .withColumn("dept_subject_to_SLA", col("dept_subject_to_SLA") == "YES")
    )

    return df

    
def join_source_data(df, df_source):
    df = df.join(df_source, "source_id", "left")
    return df

def wrangle_data():
    df_source, df, dept = read_csv()
    df = turn_values_to_bools(df)
    df = turn_district_string(df)
    df = edit_address(df)
    df = create_days_late_to_weeks(df)
    df = change_to_date(df)
    df = create_case_age(df)
    df = join_dept_data(df, dept)
    df = join_source_data(df, df_source)

    return df