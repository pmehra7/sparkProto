package com.condenast.examples

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]):Unit = {

    val spark = SparkSession
      .builder
      .appName("ProtoBuff Example")
      .config("spark.cassandra.output.ignoreNulls", "true")
      .enableHiveSupport()
      .getOrCreate


  }
}
