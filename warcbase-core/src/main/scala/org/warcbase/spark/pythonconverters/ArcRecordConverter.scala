/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
