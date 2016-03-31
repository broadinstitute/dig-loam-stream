package tools.core

import java.nio.file.Path

import loamstream.util.FileAsker
import tools.core.CoreConfig.{SampleFilePathFun, SingletonFilePathFun, VcfFilePathFun}

/**
  * LoamStream
  * Created by oliverr on 3/31/2016.
  */
object CoreConfig {

  trait VcfFilePathFun extends (String => Path)

  trait SampleFilePathFun {
    def get: Path
  }

  trait SingletonFilePathFun {
    def get: Path
  }

  object InteractiveConfig extends CoreConfig {
    override val getVcfFilePathFun: VcfFilePathFun = new VcfFilePathFun {
      override def apply(id: String): Path = FileAsker.ask("VCF file '" + id + "'")
    }

    override def getSampleFilePathFun: SampleFilePathFun = new SampleFilePathFun {
      override def get: Path = FileAsker.ask("samples file")
    }

    override def getSingletonFilePathFun: SingletonFilePathFun = new SingletonFilePathFun {
      override def get: Path = FileAsker.ask("singleton counts file")
    }
  }

  case class InteractiveFallbackConfig(vcfFiles: Seq[String => Path], sampleFiles: Seq[Path],
                                       singletonFiles: Seq[Path])
    extends CoreConfig {
    override val getVcfFilePathFun: VcfFilePathFun = new VcfFilePathFun {
      override def apply(id: String): Path = FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")
    }

    override def getSampleFilePathFun: SampleFilePathFun = new SampleFilePathFun {
      override def get: Path = FileAsker.askIfParentDoesNotExist(sampleFiles)("samples file")
    }

    override def getSingletonFilePathFun: SingletonFilePathFun = new SingletonFilePathFun {
      override def get: Path = FileAsker.askIfParentDoesNotExist(singletonFiles)("singleton file")
    }
  }

}

trait CoreConfig {
  def getVcfFilePathFun: VcfFilePathFun

  def getSampleFilePathFun: SampleFilePathFun

  def getSingletonFilePathFun: SingletonFilePathFun

  val genotypesId = "genotypes"
  val vdsId = "variant dataset"
  val singletonsId = "singleton counts"
  val pcaWeightsId = "pcaWeights"
}

