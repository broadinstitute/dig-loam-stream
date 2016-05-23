package loamstream.tools.core

import java.nio.file.{Files, Path, Paths}

import loamstream.LEnv
import loamstream.LEnv.Key
import loamstream.tools.klusta.KlustaKwikKonfig
import loamstream.util.FileAsker

import scala.language.implicitConversions

/**
  * LoamStream
  * Created by oliverr on 4/1/2016.
  */
object LCoreEnv {

  object Keys {
    val nameOfThisObject = "loamstream.tools.core.LCoreEnv.Keys"
    val vcfFilePath = Key[String => Path]("VCF file path")
    val sampleFilePath = Key[() => Path]("sample file path")
    val singletonFilePath = Key[() => Path]("singleton file path")
    val pcaWeightsFilePath = Key[() => Path]("PCA weights file path")
    val klustaKwikKonfig = Key[KlustaKwikKonfig]("KlustaKwik konfig")
    val pcaProjectionsFilePath = Key[() => Path]("PCA projections file path")
    val clusterFilePath = Key[() => Path]("PCA projections file path")
    val genotypesId = Key[String]("genotypes id")
    val vdsId = Key[String]("variant dataset")
    val singletonsId = Key[String]("singleton counts")
    val pcaWeightsId = Key[String]("pcaWeights")
  }

  object Helpers {
    val nameOfThisObject = "loamstream.tools.core.LCoreEnv.Helpers"

    def path(pathString: String): Path = Paths.get(pathString)

    def tempFile(prefix: String, suffix: String): () => Path = tempFileProvider(prefix, suffix)

    def tempDir(prefix: String): () => Path = tempDirProvider(prefix)
  }

  object Implicits {
    val nameOfThisObject = "loamstream.tools.core.LCoreEnv.Implicits"

    implicit def toConstantFunction[T](item: T): () => T = () => item
  }

  def tempFileProvider(prefix: String, suffix: String): () => Path = () => Files.createTempFile(prefix, suffix)

  def tempDirProvider(prefix: String): () => Path = () => Files.createTempDirectory(prefix)

  object FilesInteractive {
    def envVcf: LEnv = LEnv(Keys.vcfFilePath -> ((id: String) => FileAsker.ask("VCF file '" + id + "'")))

    def envSamples: LEnv = LEnv(Keys.sampleFilePath -> (() => FileAsker.ask("samples file")))

    def envSingletons: LEnv = LEnv(Keys.singletonFilePath -> (() => FileAsker.ask("singleton counts file")))

    def env: LEnv = envVcf ++ envSamples ++ envSingletons
  }

  object FileInteractiveFallback {
    def envVcf(vcfFiles: Seq[String => Path]): LEnv =
      LEnv(Keys.vcfFilePath ->
        ((id: String) => FileAsker.askIfNotExist(vcfFiles.map(_ (id)))("VCF file '" + id + "'")))

    def envSamples(sampleFiles: Seq[Path]): LEnv =
      LEnv(Keys.sampleFilePath -> (() => FileAsker.askIfParentDoesNotExist(sampleFiles)("samples file")))

    def envSingletons(singletonFiles: Seq[Path]): LEnv =
      LEnv(Keys.singletonFilePath -> (() => FileAsker.askIfParentDoesNotExist(singletonFiles)("singleton file")))

    def env(vcfFiles: Seq[String => Path], sampleFiles: Seq[Path], singletonFiles: Seq[Path]): LEnv =
      envVcf(vcfFiles) ++ envSamples(sampleFiles) ++ envSingletons(singletonFiles)
  }

}
