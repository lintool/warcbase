package org.warcbase.spark.pythonconverters

import scala.collection.JavaConversions._
import scala.util.parsing.json.JSONObject

import org.warcbase.data.ArcRecordUtils;
import org.warcbase.io.ArcRecordWritable
import org.apache.hadoop.io.{MapWritable, Text}
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

class ArcRecordWritableToMetadataConverter extends Converter[Any, java.util.Map[String, String]] {
  override def convert(obj: Any): java.util.Map[String, String] = {
    val key = obj.asInstanceOf[ArcRecordWritable]
    val meta = key.getRecord().getMetaData()

    mapAsJavaMap(Map[String, String](
      "url" -> meta.getUrl(),
      "date" -> meta.getDate(),
      "mime-type" -> meta.getMimetype()
    ))
  }
}

class ArcRecordWritableToStringMetadataConverter extends Converter[Any, String] {
  override def convert(obj: Any): String = {
    val key = obj.asInstanceOf[ArcRecordWritable]
    val meta = key.getRecord().getMetaData()
    meta.getUrl() + "\t" + meta.getDate() + "\t" + meta.getMimetype()
  }
}

class ArcRecordWritableToHtmlConverter extends Converter[Any, java.util.Map[String, String]] {
  override def convert(obj: Any): java.util.Map[String, String] = {
    val key = obj.asInstanceOf[ArcRecordWritable]
    val meta = key.getRecord().getMetaData()

    if ( meta.getMimetype() != "text/html" ) {
      null
    } else {
      mapAsJavaMap(Map[String, String](
        "url" -> meta.getUrl(),
        "date" -> meta.getDate(),
        "mime-type" -> meta.getMimetype(),
        "content" -> new String(ArcRecordUtils.getBodyContent(key.getRecord()))
      ))
    }
  }
}
