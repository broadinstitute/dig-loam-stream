package loamstream.loam.intake.flip

import java.io.FileReader
import loamstream.util.CanBeClosed
import java.io.Reader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream

/**
 * @author clint
 * Apr 1, 2020
 */
final class ReferenceFileHandle(makeNewReader: => Reader) {
  def readAt(i: Long): Option[Char] = {
    CanBeClosed.enclosed(makeNewReader) { reader =>
      val numSkipped = reader.skip(i)
    
      if(numSkipped == i) {
        val ch = reader.read()
        
        if(ch >= 0) { Some(ch.toChar) }
        else { None }
      } 
      else { None }
    }
  }
  
  def readAt(start: Long, length: Int): Option[String] = {
    val arr: Array[Char] = Array.ofDim(length)
    
    CanBeClosed.enclosed(makeNewReader) { reader =>
      def read(): Option[String] = {
        val numRead = reader.read(arr, 0, length)
      
        if(numRead == length) Some(arr.mkString) else None
      }
      
      val numSkipped = reader.skip(start)
      
      if(numSkipped == start) read() else None
    }
  }
}

object ReferenceFileHandle {
  def apply(file: java.io.File): ReferenceFileHandle = {
    if(file.toString.endsWith("gz")) { ReferenceFileHandle.fromGzippedFile(file) }
    else { new ReferenceFileHandle(new FileReader(file)) }
  }
  
  def fromGzippedFile(file: java.io.File): ReferenceFileHandle = {
    new ReferenceFileHandle(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))
  }
}
