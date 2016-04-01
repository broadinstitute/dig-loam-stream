package tools.core

import java.nio.file.Path

import loamstream.LEnv
import loamstream.LEnv.Key
import loamstream.util.FileAsker

/**
  * LoamStream
  * Created by oliverr on 4/1/2016.
  */
object LCoreEnv {

  object Keys {
    val vcfFilePath = Key[VcfFilePathVal]("VCF file path")
    val sampleFilePath = Key[SampleFilePathVal]("sample file path")
    val singletonFilePath = Key[SingletonFilePathVal]("singleton file path")
    val genotypesId = Key[String]("genotypes id")
    val vdsId = Key[String]("variant dataset")
    val singletonsId = Key[String]("singleton counts")
    val pcaWeightsId = Key[String]("pcaWeights")
  }

  trait VcfFilePathVal extends (String => Path)

  trait SampleFilePathVal {
    def get: Path
  }

  trait SingletonFilePathVal {
    def get: Path
  }

  def envVcfFileInteractive: LEnv = LEnv(Keys.vcfFilePath -> new VcfFilePathVal {
    override def apply(id: String): Path = FileAsker.ask("VCF file '" + id + "'")
  })

  def envVcfFileInteractiveFallback(vcfFiles: Seq[String => Path]): LEnv =
    LEnv(Keys.vcfFilePath -> new VcfFilePathVal {
      override def apply(id: String): Path = FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")
    })

  def envSampleFileInteractive: LEnv = LEnv(Keys.sampleFilePath -> new SampleFilePathVal {
    override def get: Path = FileAsker.ask("samples file")
  })

  def envSampleFileInteractiveFallback(sampleFiles: Seq[Path]): LEnv =
    LEnv(Keys.sampleFilePath -> new SampleFilePathVal {
      override def get: Path = FileAsker.askIfParentDoesNotExist(sampleFiles)("samples file")
    })

  def envSingletonFileInteractive: LEnv = LEnv(Keys.singletonFilePath -> new SingletonFilePathVal {
    override def get: Path = FileAsker.ask("singleton counts file")
  })

  def envSingletonFileInteractiveFallback(singletonFiles: Seq[Path]): LEnv =
    LEnv(Keys.singletonFilePath -> new SingletonFilePathVal {
      override def get: Path = FileAsker.askIfParentDoesNotExist(singletonFiles)("singleton file")
    })

}
