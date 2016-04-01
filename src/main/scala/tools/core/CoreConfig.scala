package tools.core

import java.nio.file.Path

import loamstream.LEnv
import loamstream.LEnv.{Key, LMapEnv}
import loamstream.util.FileAsker
import tools.core.CoreConfig.SingletonFilePathVal

/**
  * LoamStream
  * Created by oliverr on 3/31/2016.
  */
object CoreConfig {

  object Keys {
    val vcfFilePath = Key[VcfFilePathVal]("VCF file path")
    val sampleFilePath = Key[SampleFilePathVal]("sample file path")
    val singletonFilePath = Key[SingletonFilePathVal]("singleton file path")
  }

  trait VcfFilePathVal extends (String => Path)

  trait SampleFilePathVal {
    def get: Path
  }

  trait SingletonFilePathVal {
    def get: Path
  }

  object InteractiveConfig extends FileBasedCoreConfig {
    override def vcfFilePathVal: VcfFilePathVal = new VcfFilePathVal {
      override def apply(id: String): Path = FileAsker.ask("VCF file '" + id + "'")
    }

    override def sampleFilePathVal: SampleFilePathVal = new SampleFilePathVal {
      override def get: Path = FileAsker.ask("samples file")
    }

    override def singletonFilePathVal: SingletonFilePathVal = new SingletonFilePathVal {
      override def get: Path = FileAsker.ask("singleton counts file")
    }
  }

  case class InteractiveFallbackConfig(vcfFiles: Seq[String => Path], sampleFiles: Seq[Path],
                                       singletonFiles: Seq[Path])
    extends FileBasedCoreConfig {
    override def vcfFilePathVal: VcfFilePathVal = new VcfFilePathVal {
      override def apply(id: String): Path = FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")
    }

    override def sampleFilePathVal: SampleFilePathVal = new SampleFilePathVal {
      override def get: Path = FileAsker.askIfParentDoesNotExist(sampleFiles)("samples file")
    }

    override def singletonFilePathVal: SingletonFilePathVal = new SingletonFilePathVal {
      override def get: Path = FileAsker.askIfParentDoesNotExist(singletonFiles)("singleton file")
    }
  }

  trait FileBasedCoreConfig extends CoreConfig {
    def vcfFilePathVal: VcfFilePathVal

    def sampleFilePathVal: SampleFilePathVal

    def singletonFilePathVal: SingletonFilePathVal

    val env = LMapEnv(Map(Keys.vcfFilePath -> vcfFilePathVal, Keys.sampleFilePath -> sampleFilePathVal))
  }

}


trait CoreConfig {
  def env: LEnv

  def singletonFilePathVal: SingletonFilePathVal

  val genotypesId = "genotypes"
  val vdsId = "variant dataset"
  val singletonsId = "singleton counts"
  val pcaWeightsId = "pcaWeights"
}

