package loamstream.loam.intake.flip

import java.io.FileReader
import loamstream.util.CanBeClosed
import java.io.Reader
import java.io.InputStreamReader
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import java.nio.file.Path
import java.lang.ref.WeakReference
import java.io.InputStream
import org.apache.commons.io.IOUtils

/**
 * @author clint
 * Apr 1, 2020
 */
trait ReferenceFileHandle {
  def readAt(i: Long): Option[Char]
  
  def readAt(start: Long, length: Int): Option[String]
}

object ReferenceFileHandle {
  def apply(file: java.io.File, inMemory: Boolean = false): ReferenceFileHandle = {
    if(file.toString.endsWith("gz")) { fromGzippedFile(file, inMemory) }
    else { fromUnzippedFile(file, inMemory) }
  }
  
  def fromUnzippedFile(file: java.io.File, inMemory: Boolean = false): ReferenceFileHandle = {
    fromStream(new FileInputStream(file), inMemory)
  }
  
  def fromGzippedFile(file: java.io.File, inMemory: Boolean = false): ReferenceFileHandle = {
    fromStream(new GZIPInputStream(new FileInputStream(file)), inMemory)
  }
  
  private def fromStream(newStream: => InputStream, inMemory: Boolean): ReferenceFileHandle = {
    if(inMemory) { new InMemory(newStream) }
    else { new OnDisk(new InputStreamReader(newStream)) }
  }
    
  final class InMemory(newStream: => InputStream) extends ReferenceFileHandle {
    private var dataRef: WeakReference[Array[Byte]] = _
    
    private def dataOpt: Option[Array[Byte]] = Option(dataRef).flatMap(ref => Option(ref.get)) 
    
    private def getData: Array[Byte] = dataOpt match {
      case Some(data) => data
      case None => {
        val data = CanBeClosed.enclosed(newStream)(IOUtils.toByteArray)
        
        dataRef = new WeakReference(data)
        
        data
      }
    }
    
    private def withData[A](f: Array[Byte] => A): A = f(getData)
    
    override def readAt(i: Long): Option[Char] = {
      require(i >= 0)
      
      withData { data =>
        if(i < data.length) { Some(data(i.toInt).toChar) }
        else { None }
      }
    }
    
    override def readAt(start: Long, length: Int): Option[String] = {
      require(start >= 0)
      
      withData { data =>
        val end = start + length
        
        val available = data.length - start
        
        if(end > data.length) { None }
        else if(available < length) { None }
        else {
          val arr: Array[Byte] = Array.ofDim(length)
          
          Array.copy(data, start.toInt, arr, 0, length)
          
          Some(arr.iterator.map(_.toChar).mkString) 
        }
      }
    }
  }
  
  final class OnDisk(makeNewReader: => Reader) extends ReferenceFileHandle {
    override def readAt(i: Long): Option[Char] = {
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
    
    override def readAt(start: Long, length: Int): Option[String] = {
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
}
