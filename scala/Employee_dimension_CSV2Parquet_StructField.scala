package vmart.parquet

// Generated on: 2017:06:06-15:57:42,333

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Employee_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Employee_dimension = StructType(
        StructField(name = "employee_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "employee_key").build())
    ::  StructField(name = "employee_gender", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_gender").build())
    ::  StructField(name = "courtesy_title", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "courtesy_title").build())
    ::  StructField(name = "employee_first_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_first_name").build())
    ::  StructField(name = "employee_middle_initial", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_middle_initial").build())
    ::  StructField(name = "employee_last_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_last_name").build())
    ::  StructField(name = "employee_age", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_age").build())
    ::  StructField(name = "hire_date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "hire_date").build())
    ::  StructField(name = "employee_street_address", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_street_address").build())
    ::  StructField(name = "employee_city", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_city").build())
    ::  StructField(name = "employee_state", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_state").build())
    ::  StructField(name = "employee_region", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "employee_region").build())
    ::  StructField(name = "job_title", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "job_title").build())
    ::  StructField(name = "reports_to", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "reports_to").build())
    ::  StructField(name = "salaried_flag", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "salaried_flag").build())
    ::  StructField(name = "annual_salary", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "annual_salary").build())
    ::  StructField(name = "hourly_rate", dataType = FloatType, nullable = true, metadata = new MetadataBuilder().putString("description", "hourly_rate").build())
    ::  StructField(name = "vacation_days", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "vacation_days").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Employee_dimension_CSV2Parquet_StructField")
      .getOrCreate()

    val df = sparkSession .read
      .format("com.databricks.spark.csv")
      .option("header", "false") //read the headers
      //      .option("mode", "FAILFAST")
      .option("mode", "DROPMALFORMED")
      .option("delimiter", "|")
      .option("inferSchema", "false")
      .option("charset", "UTF-8")
      .option("dateFormat", "MM/dd/yyyy")
      .schema(Employee_dimension)
      .load("/tmp/ros/employee_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/employee_dimension.parquet")

  }
}
