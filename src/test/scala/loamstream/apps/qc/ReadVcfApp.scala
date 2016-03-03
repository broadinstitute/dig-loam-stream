package loamstream.apps.qc

import loamstream.testdata.VcfFiles

import scala.io.{Codec, Source}

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object ReadVcfApp extends App {


  for (line <- Source.fromFile(VcfFiles.miniPath.toFile)(Codec.UTF8).getLines()) {
    println(line)
  }

}
