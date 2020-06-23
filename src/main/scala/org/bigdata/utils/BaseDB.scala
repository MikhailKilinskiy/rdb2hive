package org.bigdata.utils

import java.sql.DriverManager
import java.sql.Connection
import reflect.runtime.universe._

abstract class BaseDB[T: TypeTag] extends Serializable with Logging {

  def openConnection(implicit config: Configuration): Connection = {

    val driverName: String = {
      typeOf[T] match {
        case t if t =:= typeOf[Teradata] => "com.teradata.jdbc.TeraDriver"
        case t if t =:= typeOf[Hive] => "org.apache.hive.jdbc.HiveDriver"
      }
    }

    val url: String = {
      typeOf[T] match {
        case t if t =:= typeOf[Teradata] => config.teradataConnectionUrl
        case t if t =:= typeOf[Hive] => config.hiveConnectionUrl
      }
    }

    val user = config.user
    val password = config.password

    val connection = DriverManager.getConnection(url, user, password)

    connection
  }


}
