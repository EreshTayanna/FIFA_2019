package com.fifa.inspark

import org.apache.spark.sql.SparkSession

object FIFAMain {
  def main(args: Array[String]): Unit = {
    val mode = args(0)
    val inputPath = args(1)
    val spark = SparkSession.builder().master(mode).appName("FIFA 2019").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    DataComputaion.computeData(spark,inputPath)
  }

}
