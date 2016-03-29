package tools.core

import java.nio.file.Path

import loamstream.model.jobs.LToolBox

/**
  * LoamStream
  * Created by oliverr on 2/23/2016.
  */
trait LCoreToolBox extends LToolBox {

  def getPredefinedVcfFile(id: String): Path

  def getSampleFile: Path
         
  def getSingletonFile: Path

}
