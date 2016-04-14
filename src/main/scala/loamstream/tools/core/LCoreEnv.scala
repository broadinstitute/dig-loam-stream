package loamstream.tools.core

import java.nio.file.Path
import loamstream.LEnv
import loamstream.LEnv.Key
import loamstream.util.FileAsker
import loamstream.tools.klusta.KlustaKwikKonfig

/**
  * LoamStream
  * Created by oliverr on 4/1/2016.
  */
object LCoreEnv {

  object Keys {
    val vcfFilePath = Key[PathProviderById]("VCF file path")
    val sampleFilePath = Key[PathProvider]("sample file path")
    val singletonFilePath = Key[PathProvider]("singleton file path")
    val pcaWeightsFilePath = Key[PathProvider]("PCA weights file path")
    val klustaKwikKonfig = Key[KlustaKwikKonfig]("KlustaKwik konfig")
    val pcaProjectionsFilePath = Key[PathProvider]("PCA projections file path")
    val clusterFilePath = Key[PathProvider]("PCA projections file path")
    val genotypesId = Key[String]("genotypes id")
    val vdsId = Key[String]("variant dataset")
    val singletonsId = Key[String]("singleton counts")
    val pcaWeightsId = Key[String]("pcaWeights")
  }

  trait PathProviderById extends (String => Path)

  trait PathProvider {
    def get: Path
  }

  object FilesInteractive {
    def envVcf: LEnv = LEnv(Keys.vcfFilePath -> new PathProviderById {
      override def apply(id: String): Path = FileAsker.ask("VCF file '" + id + "'")
    })

    def envSamples: LEnv = LEnv(Keys.sampleFilePath -> new PathProvider {
      override def get: Path = FileAsker.ask("samples file")
    })

    def envSingletons: LEnv = LEnv(Keys.singletonFilePath -> new PathProvider {
      override def get: Path = FileAsker.ask("singleton counts file")
    })

    def env: LEnv = envVcf ++ envSamples ++ envSingletons
  }

  object FileInteractiveFallback {
    def envVcf(vcfFiles: Seq[String => Path]): LEnv =
      LEnv(Keys.vcfFilePath -> new PathProviderById {
        override def apply(id: String): Path = FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")
      })

    def envSamples(sampleFiles: Seq[Path]): LEnv =
      LEnv(Keys.sampleFilePath -> new PathProvider {
        override def get: Path = FileAsker.askIfParentDoesNotExist(sampleFiles)("samples file")
      })

    def envSingletons(singletonFiles: Seq[Path]): LEnv =
      LEnv(Keys.singletonFilePath -> new PathProvider {
        override def get: Path = FileAsker.askIfParentDoesNotExist(singletonFiles)("singleton file")
      })

    def env(vcfFiles: Seq[String => Path], sampleFiles: Seq[Path], singletonFiles: Seq[Path]): LEnv =
      envVcf(vcfFiles) ++ envSamples(sampleFiles) ++ envSingletons(singletonFiles)
  }

}
