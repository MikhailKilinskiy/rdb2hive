package org.bigdata.utils

import org.yaml.snakeyaml.Yaml
import java.io.FileInputStream


case class Configuration (hiveConnectionUrl: String,
                          teradataConnectionUrl: String,
                          user: String,
                          password: String,
                          teradataSource: String,
                          hiveSink: String,
                          partitionColumns: String,
                          partitionValues: String,
                          fs: String
                         )


object Configuration extends Serializable with Logging {
  def build (pathToConfig: String): Configuration = {
    val yaml = new Yaml()
    logger.warn(s"Load config file $pathToConfig")
    val conf: java.util.LinkedHashMap[String, Object] = yaml.load(new FileInputStream(pathToConfig))

      Configuration(
        hiveConnectionUrl = conf.get("hive_connection").asInstanceOf[String],
        teradataConnectionUrl = conf.get("teradata_connection").asInstanceOf[String],
        user = conf.get("user").asInstanceOf[String],
        password = conf.get("password").asInstanceOf[String],
        teradataSource = conf.get("teradata_source").asInstanceOf[String],
        hiveSink = conf.get("hive_sink").asInstanceOf[String],
        partitionColumns = conf.get("parttion_column").asInstanceOf[String],
        partitionValues = conf.get("partition_values").asInstanceOf[String],
        fs = conf.get("fs").asInstanceOf[String]
    )
  }
}
