package tools

import java.io.File

import loamstream.conf.SampleFiles
import utils.{FileUtils, Loggable}

/**
  * LoamStream
  * Created by oliverr on 3/7/2016.
  */
object SampleExtractorApp extends App with Loggable {
  (SampleFiles.miniVcfOpt, SampleFiles.samplesOpt) match {
    case (Some(vcfFile), Some(samplesFile)) =>
      val samples = VcfParser(vcfFile).readSamples
      FileUtils.printToFile(new File("samples.txt")) {
        p => samples.foreach(p => debug(p.toString))
      }
    case _ => ()
  }
}
