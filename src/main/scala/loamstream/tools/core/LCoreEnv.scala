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

  object Helpers {
    val nameOfThisObject = "loamstream.tools.core.LCoreEnv.Helpers"

    def path(pathString: String): Path = Paths.get(pathString)

    def tempFile(prefix: String, suffix: String): PathProvider = tempFileProvider(prefix, suffix)

    def tempDir(prefix: String): PathProvider = tempDirProvider(prefix)
  }

  object Implicits {
    val nameOfThisObject = "loamstream.tools.core.LCoreEnv.Implicits"

    implicit def pathToPathProvider(path: Path): PathProvider = PathProviderConst(path)
  }

  trait PathProviderById extends (String => Path)

  trait PathProvider {
    def get: Path
  }

  case class PathProviderConst(get: Path) extends PathProvider

  def tempFileProvider(prefix: String, suffix: String) = new PathProvider {
    override def get: Path = Files.createTempFile(prefix, suffix)
  }

  def tempDirProvider(prefix: String) = new PathProvider {
    override def get: Path = Files.createTempDirectory(prefix)
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
