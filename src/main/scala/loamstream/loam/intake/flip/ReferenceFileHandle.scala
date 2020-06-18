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
 * 
 * A trait to represent a reference (.fasta) file.  There are two implementations: InMemory, which loads whole
 * reference files into memory as byte arrays and is very fast, and OnDisk, which doesn't load files into memory
 * and is significantly slower.
 * 
 * Both files make the same assumptions that the Perl code this is based on do: that the reference files contain 
 * a sequence of 1-byte characters with no gaps, where the index of a byte in the file directly corresponds to a
 * position on the reference genome.  The reference files we have for this purpose fit the above, and are technically
 * .fasta files, but no attempt is made here (or in the original Perl code) to handle fasta-format features like 
 * comments, spaces, multiple lines, etc etc. 
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
    //Use a WeakReference to store the data in a reference file, so that it may be garbage-collected.
    //Since most input files are broadly sorted by chromosome, this means it's likely that only one reference
    //file will be needed at once.  Allowing the files' data to ge GC'd means that only hundreds of megs need
    //stay on the heap at any time, instead of ~4GB.
    private var dataRef: WeakReference[Array[Byte]] = _
    
    private def dataOpt: Option[Array[Byte]] = Option(dataRef).flatMap(ref => Option(ref.get)) 
    
    private def getData: Array[Byte] = dataOpt match {
      case Some(data) => data
      case None => {
        val data: Array[Byte] = CanBeClosed.enclosed(newStream)(IOUtils.toByteArray)
        
        dataRef = new WeakReference(data)
        
        data
      }
    }
    
    private def withData[A](f: Array[Byte] => A): A = f(getData)
    
    override def readAt(i: Long): Option[Char] = {
      require(i >= 0)
      
      withData { data =>
        //Note i.toInt, since JVM array indices are ints :\
        //Note .toChar, which assumes that the bytes loaded from the reference files represent characters convertible
        //to JVM (unicode) chars.  (Ultimately this boils down to something like Numeric[Byte].toInt.toChar .)
        //This is the same assumption (basically) as was made by the Perl code this is based on.
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
