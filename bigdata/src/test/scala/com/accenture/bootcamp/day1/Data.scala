package com.accenture.bootcamp.day1

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Data {

  val spark: SparkSession = SparkSession.builder()
    .appName("BigDataBootcampDay1")
    .master("local")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.driver.host", "localhost")
    .getOrCreate()

  protected val logger: Logger = {
    val l = LogManager.getRootLogger
    l.setLevel(Level.DEBUG)

    val org = LogManager.getLogger("org")
    org.setLevel(Level.OFF)

    l
  }

  val newYearHonours: RDD[String] = Loader.loadNewYearHonours(spark.sparkContext)
  val australianTreaties: RDD[String] = Loader.loadAustralianTreaties(spark.sparkContext)



}
