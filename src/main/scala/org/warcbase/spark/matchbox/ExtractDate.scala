package org.warcbase.spark.matchbox

object ExtractDate {

  object Date extends Enumeration {
    type Date = Value
    val YYYY, MM, DD, YYYYMM, YYYYMMDD = Value
  }

  import Date._

  def apply(fullDate: String, d: Date): String =
    if (fullDate == null) fullDate
    else d match {
      case YYYY => fullDate.substring(0, 4)
      case MM => fullDate.substring(4, 6)
      case DD => fullDate.substring(6, 8)
      case YYYYMM => fullDate.substring(0, 6)
      case YYYYMMDD => fullDate.substring(0, 8)
    }
}
