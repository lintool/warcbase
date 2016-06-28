package org.warcbase.spark.matchbox

/**
  * Simple wrapper for getting different parts of a date
  */
object ExtractDate {

  object DateComponent extends Enumeration {
    type DateComponent = Value
    val YYYY, MM, DD, YYYYMM, YYYYMMDD = Value
  }

  import DateComponent._

  /**
    * Extracts the wanted component from a date
    *
    * @param fullDate date returned by `WARecord.getCrawlDate`, formatted as YYYYMMDD
    * @param dateFormat an enum describing the portion of the date wanted
    */
  def apply(fullDate: String, dateFormat: DateComponent): String =
    if (fullDate == null) fullDate
    else dateFormat match {
      case YYYY => fullDate.substring(0, 4)
      case MM => fullDate.substring(4, 6)
      case DD => fullDate.substring(6, 8)
      case YYYYMM => fullDate.substring(0, 6)
      case YYYYMMDD => fullDate.substring(0, 8)
    }
}
