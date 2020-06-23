package org.bigdata

package object utils {

    sealed trait Database
    trait Teradata extends Database
    trait Hive extends Database
}
