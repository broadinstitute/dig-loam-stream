package loamstream.conf

import utils.Loggable

import scala.io.{Codec, Source}

/**
  * LoamStream
  * Created by oliverr on 3/2/2016.
  */
object ReadVcfApp extends App with Loggable {


  for (file <- SampleFiles.miniVcfOpt; line <- Source.fromFile(file.toFile)(Codec.UTF8).getLines()) {
    debug(line)
  }

  debug(LProperties.properties.getProperty(SampleFiles.PropertyKeys.miniVcf))

  debug(SampleFiles.miniVcfOpt.toString)

}
