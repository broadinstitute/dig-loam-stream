package loamstream.apps.minimal

import java.nio.file.Path

import loamstream.model.jobs.LToolBox

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
trait LBasicToolBox extends LToolBox {

  def getPredefindedVcfFile(id: String): Path

  def getSampleFile: Path

}
