package org.bigdata.utils

import java.sql.Connection
import java.sql.{ResultSet, ResultSetMetaData}
import java.sql.Statement

import scala.util.{Failure, Success, Try}


class HiveUtil(implicit config: Configuration) extends BaseDB[Hive] {
  @transient
  lazy val connection: Connection = openConnection

  def createTableIfNotExists(rs: ResultSet): Unit = {
    val meta: ResultSetMetaData = rs.getMetaData
    val sb = new StringBuilder()
    sb.append(s"CREATE TABLE IF NOT EXISTS ${config.hiveSink} (")

    for(id <- 1 until meta.getColumnCount) {
      val colName: String = meta.getColumnName(id).toLowerCase
      val colType: String = meta.getColumnTypeName(id)
      if (!colName.equals(config.partitionColumns.toLowerCase)) {
        colType match {
          case "TINYINT" | "SMALLINT" | "BYTEINT" | "BIGINT" | "INTEGER" =>
            sb.append(s"${colName.toLowerCase} int")
          case "DECIMAL" =>
            val precision = meta.getPrecision(id)
            val scale = meta.getScale(id)
            sb.append(s"${colName.toLowerCase} decimal($precision, $scale)")
          case "CHAR" | "VARCHAR" =>
            val precision = meta.getPrecision(id)
            sb.append(s"${colName.toLowerCase} ${colType.toLowerCase}($precision)")
          case _ =>
            sb.append(s"${colName.toLowerCase} ${colType.toLowerCase}")
        }
        if (id != meta.getColumnCount - 1) sb.append(",\n") else sb.append(")\n")
      }
    }
    sb.append(s"PARTITIONED BY (${config.partitionColumns.toLowerCase} date)\n")
    sb.append("ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'\n")
    sb.append("STORED AS ORC")
    val sql = sb.toString()
    try {
      val stm: Statement = connection.createStatement()
      stm.executeUpdate(sql)
    }
    catch {
      case e: Exception => throw e
    }
    finally {
      if(rs != null && !rs.isClosed) rs.close()
      connection.close()
    }
  }

  def dropPartitionIfExists(partitionValue: String): Unit = Try{
    logger.info(s"Try to drop partition $partitionValue")
    val sql = s"ALTER TABLE ${config.hiveSink} DROP IF EXISTS PARTITION(${config.partitionColumns.toLowerCase}=$partitionValue)"
    val stm: Statement = connection.createStatement()
    stm.execute(sql)
  } match {
    case Success(value) => value
    case Failure(exception) => throw exception
  }

  def addPartition(partitionValue: String): Unit = Try{
    logger.info(s"Try to add partition $partitionValue")
    val sql = s"ALTER TABLE ${config.hiveSink} ADD PARTITION (${config.partitionColumns.toLowerCase}=$partitionValue)"
    val stm: Statement = connection.createStatement()
    stm.execute(sql)
  } match {
    case Success(value) => value
    case Failure(exception) => throw exception
  }

  def loadData(filePath: String, partitionValue: String): Unit = Try{
    logger.info("Try to load data int partition")
    val sb = new StringBuilder()
    sb.append(s"LOAD DATA INPATH '$filePath' OVERWRITE\n")
    sb.append(s"INTO TABLE ${config.hiveSink} PARTITION (${config.partitionColumns.toLowerCase}=$partitionValue)")
    val sql = sb.toString()
    val stm: Statement = connection.createStatement()
    stm.execute(sql)
  } match {
    case Success(value) => value
    case Failure(exception) => throw exception
  }









}
