package loamstream.loam.intake.flip

import java.nio.file.Files.exists
import java.nio.file.Path

/**
 * @author clint
 * Apr 1, 2020
 */
final class ReferenceFiles private[flip] (chromsToFiles: Map[String, ReferenceFileHandle]) {
  def isKnown(chrom: String): Boolean = chromsToFiles.contains(chrom)
  
  def getChar(chrom: String, position: Long): Option[Char] = chromsToFiles.get(chrom).flatMap(_.readAt(position))
  
  def getString(chrom: String, position: Long, length: Int): Option[String] = {
    chromsToFiles.get(chrom).flatMap(_.readAt(position, length))
  }
  
  def forChrom(chrom: String): ReferenceFileHandle = chromsToFiles(chrom)
}
  
object ReferenceFiles {
  private[flip] val empty: ReferenceFiles = new ReferenceFiles(Map.empty)
  
  def apply(referenceDir: Path, knownChroms: Set[String]): ReferenceFiles = {
    new ReferenceFiles(Map.empty ++ {
      for {
        chrom <- knownChroms.iterator
      } yield {
        val path = referenceDir.resolve(s"${chrom}.txt")
        
        require(exists(path), s"ERROR: no sequence file for chromosome: ${chrom} (${path} not found)")
        
        chrom -> new ReferenceFileHandle(path.toFile)
      }
    })
  }
}
