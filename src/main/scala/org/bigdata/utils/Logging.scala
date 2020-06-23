package org.bigdata.utils

import org.apache.log4j.Logger

trait Logging extends Serializable {
  @transient
  final protected lazy val logger: Logger = Logger.getLogger(getClass)
}

