package tools.core

import java.nio.file.Path

import loamstream.LEnv
import tools.core.LCoreEnv.Keys

/**
  * LoamStream
  * Created by oliverr on 3/31/2016.
  */
object CoreConfig {

  object InteractiveConfig extends FileBasedCoreConfig {
    override def envVcfFile: LEnv = LCoreEnv.envVcfFileInteractive

    override def envSampleFile: LEnv = LCoreEnv.envSampleFileInteractive

    override def envSingletonFile: LEnv = LCoreEnv.envSingletonFileInteractive
  }

  case class InteractiveFallbackConfig(vcfFiles: Seq[String => Path], sampleFiles: Seq[Path],
                                       singletonFiles: Seq[Path])
    extends FileBasedCoreConfig {
    override def envVcfFile: LEnv = LCoreEnv.envVcfFileInteractiveFallback(vcfFiles)

    override def envSampleFile: LEnv = LCoreEnv.envSampleFileInteractiveFallback(sampleFiles)

    override def envSingletonFile: LEnv = LCoreEnv.envSingletonFileInteractiveFallback(singletonFiles)
  }

  trait FileBasedCoreConfig extends CoreConfig {

    def envVcfFile: LEnv

    def envSampleFile: LEnv

    def envSingletonFile: LEnv

    val env = envVcfFile ++ envSampleFile ++ envSingletonFile ++ LEnv(Keys.genotypesId -> "genotypes",
      Keys.vdsId -> "variant dataset", Keys.singletonsId -> "singleton counts", Keys.pcaWeightsId -> "pcaWeights")
  }

}


trait CoreConfig {
  def env: LEnv
}

