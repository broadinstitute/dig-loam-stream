package loamstream.util

import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest

import java.nio.file.Path
import java.nio.file.{ Paths => JPaths }

/**
 * @author clint
 * date: Aug 1, 2016
 * 
 * Methods for hashing paths and chunks of bytes
 */
object Hashes {

  //TODO: Review this value. It was chosen arbitrarily; another one may be better for performance or other reasons.
  private val chunkSize = 4096
  
  def sha1(path: Path): Hash = hash(HashType.Sha1, path)

  def sha1(data: Array[Byte]): Hash = sha1(Iterator(data))

  def sha1(data: Iterator[Array[Byte]]): Hash = digest(HashType.Sha1)(data)
  
  def md5(path: Path): Hash = hash(HashType.Md5, path)
  
  /**
   * If the path is a file, hash its bytes and return the result; if the path is a dir,
   * compute its hash by building a Merkle Tree (https://en.wikipedia.org/wiki/Merkle_tree)
   */
  private def hash(hashType: HashType, path: Path): Hash = {
    require(path != null) //scalastyle:ignore
    
    val file = path.toFile

    require(file.exists, s"'$file' does not exist!")
    
    val chunks = {
      if(file.isDirectory) { dirChunks(hashType, file) } 
      else { fileChunks(hashType, file) }
    }
    
    digest(hashType)(chunks)
  }
  
  private def dirChunks(hashType: HashType, dir: File): Iterator[Array[Byte]] = {
    val children = dir.listFiles
    
    def toPath(f: File) = JPaths.get(f.toURI)
    
    //Avoid null to appease scalastyle
    Option(children) match {
      case Some(_) =>
        //Sort files so that the order of files in a dir will always be the same across traversals
        children.sortBy(_.getAbsolutePath).iterator.flatMap { child =>
          if(child.isDirectory) { dirChunks(hashType, child) }
          else { Iterator(hash(hashType, toPath(child)).value.toArray) }
        }
      case None => Iterator.empty
    }
  }
  
  private def fileChunks(hashType: HashType, file: File): Iterator[Array[Byte]] = {
    val in = new FileInputStream(file)
    
    def read(): Array[Byte] = {
      val buf: Array[Byte] = Array.ofDim(chunkSize)

      val count = in.read(buf)

      if(count == chunkSize) buf else buf.take(count)
    }

    def notDone(buf: Array[Byte]): Boolean = buf.length > 0

    val chunks = Iterator.continually(read()).takeWhile(notDone)

    //Return an iterator that lazily closes the InputStream when the iterator is exhausted
    TakesEndingActionIterator(chunks)(in.close())
  }
  
  def digest(hashType: HashType)(data: Iterator[Array[Byte]]): Hash = {
    val messageDigest = MessageDigest.getInstance(hashType.algorithmName)

    data.foreach(messageDigest.update)

    Hash(messageDigest.digest, hashType)
  }
}
