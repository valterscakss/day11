package com.accenture.bootcamp.day1
import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Loader {

  protected def fromResource(resource: String): String = {
    // C:\Users\Student\Desktop\bigdata_day1\bigdata\src\test\resources\1918NewYearHonours.txt
    // C:/Users/Student/Desktop/bigdata_day1/bigdata/src/test/resources1918NewYearHonours.txt
    new File(
      new File("src/test/resources/" + resource)
        .getCanonicalPath
    )
      .toURI.toString
  }

  def loadNewYearHonours(sc: SparkContext): RDD[String] = {
    // TODO Task #1: Create RDD from file `1918NewYearHonours.txt`
    val filePath = fromResource("1918NewYearHonours.txt")
    val dataRDD1= sc.textFile(filePath)
    dataRDD1

    // val dataRDD = sc.read.textFile("1918NewYearHonours.txt").rdd
  }

  def loadAustralianTreaties(sc: SparkContext): RDD[String] = {
    // TODO Task #2: Create RDD from file `ListOfAustralianTreaties.txt`
    val filePath = fromResource("ListOfAustralianTreaties.txt")
    val dataRDD2= sc.textFile(filePath)
    dataRDD2
  }


}
