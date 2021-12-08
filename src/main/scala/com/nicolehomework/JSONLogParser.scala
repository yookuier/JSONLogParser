package com.nicolehomework

import org.apache.spark.sql.functions.{col, split}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

/**
 * Author: Nicole Yu
 *
 */
object JSONLogParser extends App{
  System.setProperty("hadoop.home.dir", "D:\\Tools\\hadoop")

  val spark: SparkSession = SparkSession.builder()
    .appName("nicolehomework.com")
    .config("spark.master", "local")
    .getOrCreate()
  val schema = new StructType()
    //.add("ts", LongType, true)
    //.add("pt", LongType, true)
   // .add("si", StringType, true)
    //.add("uu", StringType, true)
   // .add("bg", StringType, true)
   // .add("sha", StringType, true)
    .add("nm", StringType, true)
   // .add("ph", StringType, true)
    //.add("dp ", IntegerType, false)

  val multiline_df = spark.read.schema(schema).json("src/main/resources/log.json")
                     .withColumn("Extension", split(col("nm"), "\\.").getItem(1))
                     .withColumn("FileName", split(col("nm"), "\\.").getItem(0)).drop("nm").dropDuplicates().drop("FileName")

  val multiline_rdd = multiline_df.rdd.countByValue().foreach(println)
}
