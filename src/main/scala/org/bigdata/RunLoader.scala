package org.bigdata

import java.sql.{ResultSet, ResultSetMetaData}
import org.bigdata.utils.{BaseDB, Configuration, TeradataUtil, HiveUtil, OrcWriter}
import org.apache.orc.TypeDescription


object RunLoader {

  def pipeline(partitionValue: String)(implicit config: Configuration): Unit = {
    import org.apache.orc.TypeDescription
    val teradataUtil = new TeradataUtil()
    val rs: ResultSet = teradataUtil.extractPartition(partitionValue)

    val orcWriter = new OrcWriter(partitionValue, rs)
    val filePath = orcWriter.write()

    val hiveUtil = new HiveUtil()
    hiveUtil.createTableIfNotExists(rs)
    hiveUtil.dropPartitionIfExists(partitionValue)
    hiveUtil.addPartition(partitionValue)
    hiveUtil.loadData(filePath, partitionValue)

  }

  def main(args: Array[String]): Unit = {
    val path = args(0)
    implicit val config = Configuration.build(path)

    val partitionValues = config.partitionValues.split(",")
    partitionValues.foreach {partitionValue =>
      pipeline(partitionValue)
    }




  }


}
