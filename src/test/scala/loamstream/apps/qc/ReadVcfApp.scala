package loamstream.apps.qc

import java.io.File

import htsjdk.variant.vcf.VCFFileReader
import utils.Loggable

import scala.collection.JavaConverters.asScalaBufferConverter

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object ReadVcfApp extends App with Loggable {


  val requireIndex = false
  //  val vcfFileReader = new VCFFileReader(VcfFiles.miniPath.toFile, requireIndex)
  val sampleVcfFile = new File("C:\\Users\\oliverr\\sampleData\\v3.clean.1000.vcf.gz")
  val vcfFileReader = new VCFFileReader(sampleVcfFile, requireIndex)
  val samples = vcfFileReader.getFileHeader.getGenotypeSamples.asScala.toSeq
  debug(samples.toString)


}
