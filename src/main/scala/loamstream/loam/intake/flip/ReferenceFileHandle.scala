package loamstream.loam.intake.flip

import java.io.FileReader
import loamstream.util.CanBeClosed

/**
 * @author clint
 * Apr 1, 2020
 */
final class ReferenceFileHandle(file: java.io.File) {
  private def newReader = new FileReader(file)
    
  def readAt(i: Long): Option[Char] = {
    CanBeClosed.enclosed(newReader) { reader =>
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
    
    CanBeClosed.enclosed(newReader) { reader =>
      def read(): Option[String] = {
        val numRead = reader.read(arr, 0, length)
      
        if(numRead == length) Some(arr.mkString) else None
      }
      
      val numSkipped = reader.skip(start)
      
      if(numSkipped == start) read() else None
    }
  }
}
