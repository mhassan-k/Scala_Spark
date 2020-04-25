package com.spark.sparkSql
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark
import org.apache.spark.sql.functions.split
import com.databricks ;
import org.apache.spark.sql.{SaveMode, SparkSession, functions, types}

object  Pscala  {

  def main(args: Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val session = SparkSession.builder().appName("Pscala").master("local[*]").getOrCreate()

    val Segment_Schema = StructType(
      Array(StructField("dial_num", IntegerType, true)))

    val Segment = session.read.option("header", "false").schema(Segment_Schema).csv("/in/Segment/SEGMENT.csv")
    // this is example define exact number of columns
    val df_Segment = Segment.toDF()
    //println(df_Segment.show())
    //println(df_Segment.count())



    val Rules_Schema = StructType(
      Array(
        StructField("R_Service_Identifier", StringType, true),
        StructField("Service_Description", StringType, true),
        StructField("Start_time", IntegerType, true),
        StructField("End_time", IntegerType, true),
        types.StructField("Total_volume", IntegerType, true)))

    val Rules = session.read.option("header", "false").schema(Rules_Schema).csv("/input/Rules/Rules.csv")
    // this is example define exact number of columns
    val df_Rules = Rules.toDF()
    //println(df_Rules.show())
    //println(df_Rules.count())

    val Data_Schema = StructType(
      Array(
        StructField("Msisdn", IntegerType, true),
        StructField("Service_Identifier", StringType, true),
        StructField("Traffic_Volume", IntegerType, true),
        StructField("Timestamp", LongType, true)))

    val Data = session.read.option("header", "false").schema(Data_Schema).csv("/input/Data")
    // this is example define exact number of columns
    //  val df_Data =Data.toDF(D_colum_names:_*)
    val df_Data =Data.toDF()
    println(df_Data.count())
    val joined = df_Data.join(df_Segment, df_Data.col("Msisdn").equalTo(df_Segment.col("dial_num")), "Inner").orderBy("Msisdn")

    val join_dist = joined.groupBy("Msisdn", "Service_Identifier").min("Timestamp").orderBy("Msisdn")
    val join_dist_01 = join_dist.withColumnRenamed("Msisdn","g_Msisdn")
      .withColumnRenamed("Service_Identifier","g_Service_Identifier")
      .withColumnRenamed("min(Timestamp)" , "min_timestamp")

    val join_final = joined.join(join_dist_01, joined("Msisdn")===join_dist_01("g_Msisdn")
      &&joined("Service_Identifier")===join_dist_01("g_Service_Identifier")&&joined("Timestamp")===join_dist_01("min_timestamp"),
      "Inner").drop("dial_num" , "g_Msisdn" ,"g_Service_Identifier" , "min_timestamp")



    //println(joined.show())
    //println(joined.count())
    // println(join_dist.show())
    println(join_final.show())


    val joined_final =join_final.join(df_Rules, join_final.col("Service_Identifier").equalTo(df_Rules.col("R_Service_Identifier")), "Inner")
      .where(join_final.col("Traffic_Volume")> df_Rules.col("Total_volume")).
      drop("R_Service_Identifier","Service_Description","Start_time","End_time" , "Total_volume").coalesce(1)

    println(joined_final.show())

    joined_final.write.mode("overwrite").csv("/in/output")
    println("Data load in CSV files successfully")
  }
}
