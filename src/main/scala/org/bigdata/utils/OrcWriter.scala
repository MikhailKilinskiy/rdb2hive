package org.bigdata.utils

import java.sql.{ResultSet, ResultSetMetaData, SQLException}
import org.apache.orc.CompressionKind
import org.apache.orc.OrcFile
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector
import org.apache.orc.storage.ql.exec.vector.ColumnVector
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector
import org.apache.orc.storage.ql.exec.vector.LongColumnVector
import org.apache.orc.storage.common.`type`.HiveDecimal
import org.apache.orc.storage.serde2.io.HiveDecimalWritable
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.orc.TypeDescription
import org.apache.orc.Writer
import java.sql.{ResultSet, ResultSetMetaData}
import java.io.File
import java.net.URI
import scala.util.{Failure, Success, Try}


class OrcWriter(partitionValue: String, rs: ResultSet)(implicit config: Configuration) extends Serializable with Logging {

  private lazy val conf = new org.apache.hadoop.conf.Configuration()
  private val hdfs = initFileSystem()
  private final val BATCH_SIZE = 10000
  private val meta = rs.getMetaData


  def initFileSystem(): FileSystem = {
    //conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    conf.set("hadoop.security.authentication", "kerberos")
    conf.set("fs.defaultFS", config.fs)
    UserGroupInformation.setConfiguration(conf)
    UserGroupInformation.isLoginTicketBased
    FileSystem.get(conf)
  }


  private def createSchemaFromMeta(): TypeDescription = {
    import org.apache.orc.TypeDescription

    val schema: TypeDescription = TypeDescription.createStruct()
    for(id <- 1 until meta.getColumnCount){
      val colName: String = meta.getColumnName(id).toLowerCase
      val colType: String = meta.getColumnTypeName(id)
      colType match {
        case "DATE" => schema.addField(colName, TypeDescription.createDate())
        case "TIMESTAMP" => schema.addField(colName, TypeDescription.createTimestamp())
        case "BOOLEAN" => schema.addField(colName, TypeDescription.createBoolean())
        case "TINYINT" => schema.addField(colName, TypeDescription.createShort())
        case "SMALLINT" => schema.addField(colName, TypeDescription.createShort())
        case "BYTEINT" => schema.addField(colName, TypeDescription.createShort())
        case "BIGINT" => schema.addField(colName, TypeDescription.createLong())
        case "INTEGER" => schema.addField(colName, TypeDescription.createInt())
        case "VARCHAR" => schema.addField(colName, TypeDescription.createVarchar())
        case "STRING" => schema.addField(colName, TypeDescription.createString())
        case "FLOAT" => schema.addField(colName, TypeDescription.createFloat())
        case "DOUBLE" => schema.addField(colName, TypeDescription.createDouble())
        case "DECIMAL" => schema.addField(colName, TypeDescription.createDecimal())
        case "BINARY" => schema.addField(colName, TypeDescription.createBinary())
        case _ => throw new RuntimeException("Unsupported column type: " + colType)
      }
    }
    schema
  }

  private def deleteFileIfExists(filePath: String): Unit = {
    try {

      if (hdfs.exists(new Path(filePath))) {
        logger.warn(s"File in path $filePath already exists and will be deleted.")
        hdfs.delete(new Path(filePath), true)
      } else {
        logger.warn(s"Create file in path $filePath.")
        hdfs.create(new Path(filePath))
      }
      filePath
    }
    catch {
      case e: Exception => throw e
    }
  }

  private def createOrcFile(): String = {
    val sb = new StringBuilder()
    sb.append(config.fs)
    sb.append(File.separator)
    sb.append("user/makilins/")
    sb.append(partitionValue.replace("-", ""))
    sb.append(File.separator)
    sb.append(System.currentTimeMillis())
    sb.append(".orc")

    sb.toString()
  }


  def write(): String = {
    import org.apache.orc.TypeDescription
    val schema = createSchemaFromMeta()
    val filePath = createOrcFile()
    val writer = OrcFile.createWriter(
      new Path(filePath),
      OrcFile.writerOptions(conf)
      .compress(CompressionKind.ZLIB)
      .bufferSize(32 * 1024 * 1024)
      .setSchema(schema))

    val batch = schema.createRowBatch(BATCH_SIZE)

    for(id <- 0 until meta.getColumnCount){
      batch.cols(id).noNulls = false
    }

    try {

      while(rs.next()) {
        val row = batch.size + 1

        for(id <- 1 until meta.getColumnCount){
          val colName: String = meta.getColumnName(id).toLowerCase
          val colType: String = meta.getColumnTypeName(id)
          colType match {
            case "DATE"|"BOOLEAN"|"TINYINT"|"SMALLINT"|"BYTEINT"|"BIGINT"|"INTEGER" =>
              val col = batch.cols(id-1).asInstanceOf[LongColumnVector]
              col.vector(row) = rs.getLong(id)
            case "TIMESTAMP" =>
              val col = batch.cols(id-1).asInstanceOf[TimestampColumnVector]
              col.set(row, rs.getTimestamp(id))
            case "VARCHAR"|"STRING"|"BINARY" =>
              val col = batch.cols(id-1).asInstanceOf[BytesColumnVector]
              col.vector(row) = rs.getBytes(id)
            case "FLOAT"|"DOUBLE"|"BINARY" =>
              val col = batch.cols(id-1).asInstanceOf[DoubleColumnVector]
              col.vector(row) = rs.getDouble(id)
            case "DECIMAL" =>
              val col = batch.cols(id-1).asInstanceOf[DecimalColumnVector]
              col.vector(row) = new HiveDecimalWritable(HiveDecimal.create(rs.getBigDecimal(id)))
            case _ => throw new RuntimeException("Unsupported ORC column type: " + colType)
          }
          if(batch.size >= BATCH_SIZE && batch.size > 0){
            writer.addRowBatch(batch)
            batch.reset()
          }
        }
      }
      writer.close()
      filePath
    }
    catch {
      case e: Exception =>
        deleteFileIfExists(filePath)
        throw e
    }
    finally {
      if(rs != null && !rs.isClosed) rs.close()
      hdfs.close()
    }
  }
}



































































