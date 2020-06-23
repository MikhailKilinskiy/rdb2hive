package org.bigdata.utils

import java.sql.Connection
import java.sql.{ResultSet, ResultSetMetaData}
import java.sql.Statement

import scala.util.{Failure, Success, Try}


class TeradataUtil(implicit config: Configuration) extends BaseDB[Teradata] {
  @transient
  lazy val connection: Connection = openConnection

  val FETCH_SIZE = 1000

  def selectQuery(partitionValue: String): String = {
    val condition = s"where ${config.partitionColumns} = $partitionValue"
    s"select * from ${config.teradataSource} $condition"
  }

  def selectQuery(): String = {
    val condition = "where 1 < 0"
    s"select * from ${config.teradataSource} $condition"
  }

  def getTableMeta: ResultSetMetaData = Try {
    val stm: Statement = connection.createStatement()
    val sql = selectQuery()
    logger.warn(s"Try to execute query: $sql")
    val rs: ResultSet = stm.executeQuery(sql)

    rs.getMetaData

  } match {
    case Success(value) => value
    case Failure(exception) => throw new Exception(exception)
  }

  def extractPartition(partitionValue: String): ResultSet = Try {
    val stm: Statement = connection.createStatement()
    val sql = selectQuery(partitionValue)
    logger.warn(s"Try to execute query: $sql")
    val rs: ResultSet = stm.executeQuery(sql)
    rs.setFetchSize(FETCH_SIZE)

    rs

  } match {
    case Success(value) => value
    case Failure(exception) => throw new Exception(exception)
  }


}

