package org.warcbase.spark.pythonconverters

import scala.collection.JavaConversions._
import scala.util.parsing.json.JSONObject

import org.warcbase.io.ArcRecordWritable
import org.apache.spark.api.python.Converter

class ArcRecordWritableToUrlConverter extends Converter[Any, String] {
  override def convert(obj: Any): String = {
    val key = obj.asInstanceOf[ArcRecordWritable]
    val meta = key.getRecord().getMetaData()
    meta.getUrl()
  }
}

class ArcRecordWritableToCrawlDateConverter extends Converter[Any, String] {
  override def convert(obj: Any): String = {
    val key = obj.asInstanceOf[ArcRecordWritable]
    val meta = key.getRecord().getMetaData()
    meta.getDate()
  }
}
