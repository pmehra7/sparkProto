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

    val keyspace = "conde_nast"
    val table = "latest_feature_values_by_entity"
    val clusterIp = "127.0.0.1"

    val featuresRaw = spark.read.format("org.apache.spark.sql.cassandra").options(Map( "table" -> table, "keyspace" -> keyspace)).load()


  }
}
