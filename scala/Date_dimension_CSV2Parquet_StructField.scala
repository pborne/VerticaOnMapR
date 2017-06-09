package vmart.parquet

// Generated on: 2017:06:06-15:57:42,199

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, _}

object Date_dimension_CSV2Parquet_StructField {
  def main(args: Array[String]): Unit = {
    val Date_dimension = StructType(
        StructField(name = "date_key", dataType = IntegerType, nullable = false, metadata = new MetadataBuilder().putString("description", "date_key").build())
    ::  StructField(name = "date", dataType = DateType, nullable = true, metadata = new MetadataBuilder().putString("description", "date").build())
    ::  StructField(name = "full_date_description", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "full_date_description").build())
    ::  StructField(name = "day_of_week", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "day_of_week").build())
    ::  StructField(name = "day_number_in_calendar_month", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "day_number_in_calendar_month").build())
    ::  StructField(name = "day_number_in_calendar_year", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "day_number_in_calendar_year").build())
    ::  StructField(name = "day_number_in_fiscal_month", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "day_number_in_fiscal_month").build())
    ::  StructField(name = "day_number_in_fiscal_year", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "day_number_in_fiscal_year").build())
    ::  StructField(name = "last_day_in_week_indicator", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "last_day_in_week_indicator").build())
    ::  StructField(name = "last_day_in_month_indicator", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "last_day_in_month_indicator").build())
    ::  StructField(name = "calendar_week_number_in_year", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_week_number_in_year").build())
    ::  StructField(name = "calendar_month_name", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_month_name").build())
    ::  StructField(name = "calendar_month_number_in_year", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_month_number_in_year").build())
    ::  StructField(name = "calendar_year_month", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_year_month").build())
    ::  StructField(name = "calendar_quarter", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_quarter").build())
    ::  StructField(name = "calendar_year_quarter", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_year_quarter").build())
    ::  StructField(name = "calendar_half_year", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_half_year").build())
    ::  StructField(name = "calendar_year", dataType = IntegerType, nullable = true, metadata = new MetadataBuilder().putString("description", "calendar_year").build())
    ::  StructField(name = "holiday_indicator", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "holiday_indicator").build())
    ::  StructField(name = "weekday_indicator", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "weekday_indicator").build())
    ::  StructField(name = "selling_season", dataType = StringType, nullable = true, metadata = new MetadataBuilder().putString("description", "selling_season").build())
    ::  Nil
    )

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Date_dimension_CSV2Parquet_StructField")
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
      .schema(Date_dimension)
      .load("/tmp/ros/date_dimension.csv.gz")

    df.write.mode("overwrite").parquet("/tmp/vertica/data/parquet/date_dimension.parquet")

  }
}
