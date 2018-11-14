package com.condenast.examples

import java.nio.ByteBuffer

import com.datastax.driver.core.SimpleStatement
import com.datastax.driver.dse.DseSession
import com.datastax.spark.connector.cql.CassandraConnector
import com.google.protobuf.Any
import org.apache.spark.sql.{Dataset, SparkSession}

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

    case class serVal(serialized_value: Array[Byte])
    val datasetBytes: Dataset[serVal] = featuresRaw.select("serialized_value").as[serVal]
    val features = datasetBytes.map(x => Any.parseFrom(ByteBuffer.wrap(x.serialized_value)))

    val myDeletes = featuresRaw.select("marked_for_delete == True")

    case class test(tinker: String)

    val connector = CassandraConnector(featuresRaw.sparkSession.sparkContext.getConf)
    myDeletes.foreachPartition(partitionOfUsers => {
      connector.withSessionDo( {cassandraSession =>
        val dseSession = cassandraSession.asInstanceOf[DseSession]
        partitionOfUsers.foreach(delete => {
          val pk = test(delete.getString(0))
          val stmt = new SimpleStatement(s"DELETE where ${pk}")
        })
      })
    })

  }
}
