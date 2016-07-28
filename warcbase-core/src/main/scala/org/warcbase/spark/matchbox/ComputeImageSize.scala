package org.warcbase.spark.matchbox

import java.io.ByteArrayInputStream
import javax.imageio.ImageIO

/**
  * Created by youngbinkim on 7/7/16.
  */
object ComputeImageSize {
  def apply(bytes: Array[Byte]): (Int, Int) = {
    val in = new ByteArrayInputStream(bytes)

    try {
      val image = ImageIO.read(in)
      if (image == null)
        return (0, 0)
      (image.getWidth(), image.getHeight())
    } catch {
      case e: Throwable => {
        e.printStackTrace()
        return (0, 0)
      }
    }
  }
}
