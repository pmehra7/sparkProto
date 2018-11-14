package com.condenast.examples

import com.datastax.driver.core.Row
import com.datastax.driver.dse.{DseCluster, DseSession}

object test {
  def main(args: Array[String]):Unit = {

    val keyspace = "conde_nast"
    val table = "latest_feature_values_by_entity"
    val clusterIp = "127.0.0.1"


    // Java Driver Stuff here
    val clusterBuilder: DseCluster = DseCluster.builder().addContactPoint(clusterIp).build()
    val session: DseSession = clusterBuilder.connect()
    val row: Row = session.execute("select release_version from system.local").one()
    System.out.println("Release Version: " + row.getString("release_version"))
    session.close()
    clusterBuilder.close()

  }
}
