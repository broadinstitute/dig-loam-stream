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
import java.io.RandomAccessFile
import java.nio.MappedByteBuffer
import java.nio.channels.FileChannel
import scala.util.Try
import scala.util.control.NonFatal

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
  def apply(file: java.io.File): ReferenceFileHandle = new MemoryMapped(file)
  
  final class MemoryMapped(file: java.io.File) extends ReferenceFileHandle {
    private[this] val raFile = new RandomAccessFile(file, "r") 
		
    private[this] val channel = raFile.getChannel
    
    private[this] val size: Long = channel.size
    
    require(size <= Integer.MAX_VALUE, s"File '$file' is too big to be memory-mapped")
    
		private[this] val mappedBuffer: MappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, size)
		
		override def readAt(i: Long): Option[Char] = {
      require(i >= 0, s"Can't read from negative position $i in file '$file'")
      
      seekTo(i.toInt).flatMap(_ => get())
    }
    
    override def readAt(start: Long, length: Int): Option[String] = {
      require(start >= 0, s"Can't seek to negative position $start in file '$file'")
      require(length >= 0, s"Can't read a negative number of bytes ($length) from file '$file'")
      
      val arr: Array[Byte] = Array.ofDim(length)
      
      def read(): Option[String] = {
        noneIfException {
          mappedBuffer.get(arr, 0, length)
          
          Some(arr.map(_.toChar).mkString)
        }
      }

      seekTo(start.toInt).flatMap(_ => read())
    }
		
		private def noneIfException[A](f: => Option[A]): Option[A] = {
      try { f }
      catch { 
        case NonFatal(_) => None
      }
    }
		
		private def seekTo(pos: Int): Option[Unit] = {
      noneIfException {
        mappedBuffer.position(pos)
        
        Some(())
      }
    }
    
    private def get(): Option[Char] = {
      noneIfException {
        Some(mappedBuffer.get().toChar)
      }
    }
  }
}
