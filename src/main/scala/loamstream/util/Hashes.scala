package loamstream.util

import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest

import scala.Iterator
import java.nio.file.Path

/**
 * @author clint
 * date: Aug 1, 2016
 * 
 * Methods for hashing paths and chunks of bytes
 */
object Hashes {

  private val chunkSize = 4096
  
  def sha1(path: Path): Hash = hash(HashType.Sha1, path)

  def sha1(data: Array[Byte]): Hash = sha1(Iterator(data))

  def sha1(data: Iterator[Array[Byte]]): Hash = hash(HashType.Sha1)(data)
  
  private def hash(hashType: HashType, path: Path): Hash = {
    require(path != null)
    
    val file = path.toFile

    require(file.exists)
    //NB: For now
    require(file.isFile)
    
    LoamFileUtils.enclosed(new FileInputStream(file)) { in =>
      def read(): Array[Byte] = {
        val buf: Array[Byte] = Array.ofDim(chunkSize)

        val count = in.read(buf)

        if(count == chunkSize) buf else buf.take(count)
      }

      def notDone(buf: Array[Byte]): Boolean = buf.size > 0

      val chunks = Iterator.continually(read()).takeWhile(notDone)

      hash(hashType)(chunks)
    }
  }
  
  private def hash(hashType: HashType)(data: Iterator[Array[Byte]]): Hash = {
    val messageDigest = MessageDigest.getInstance(hashType.algorithmName)

    data.foreach(messageDigest.update)

    Hash(messageDigest.digest, hashType)
  }
}