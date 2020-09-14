package loamstream.loam.intake.flip

import java.nio.file.Files.exists
import java.nio.file.Path
import loamstream.util.TimeUtils
import loamstream.util.Loggable

/**
 * @author clint
 * Apr 1, 2020
 */
final class ReferenceFiles private[flip] (chromsToFiles: Map[String, ReferenceFileHandle]) extends Loggable {
  def isKnown(chrom: String): Boolean = chromsToFiles.contains(chrom)
  
  def getChar(chrom: String, position: Long): Option[Char] = chromsToFiles.get(chrom).flatMap(_.readAt(position))
  
  def getString(chrom: String, position: Long, length: Int): Option[String] = {
    chromsToFiles.get(chrom).flatMap(_.readAt(position, length))
  }
}
  
object ReferenceFiles {
  private[flip] val empty: ReferenceFiles = new ReferenceFiles(Map.empty)
  
  def apply(referenceDir: Path, knownChroms: Set[String]): ReferenceFiles = {
    new ReferenceFiles(Map.empty ++ {
      for {
        chrom <- knownChroms.iterator
      } yield {
        val txtPath = referenceDir.resolve(s"${chrom}.txt")
        val gzPath = referenceDir.resolve(s"${chrom}.gz")
        
        require(
            !(exists(txtPath) && exists(gzPath)), 
            s"ERROR: both sequence files ${txtPath} and ${gzPath} exist for chromosome: ${chrom}")
        
        require(
            exists(txtPath) || exists(gzPath), 
            s"ERROR: no sequence file for chromosome: ${chrom} (both ${txtPath} and ${gzPath} not found)")
        
        val handle = {
          //prefer unzipped files, if both zipped and unzipped are present
          val path = if(exists(txtPath)) txtPath else gzPath
          
          //NB: Memory-map reference files for performance.  These files are large, but only hundreds of megs.
          //Doing this results in a ~500x speedup compared to seeking into files with java.io.Readers.
          ReferenceFileHandle(path.toFile)
        }
        
        chrom -> handle
      }
    })
  }
}
