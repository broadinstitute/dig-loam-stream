package loamstream.tools.core

import java.nio.file.{Files, Path}

import loamstream.LEnv
import loamstream.LEnv.Key
import loamstream.loam.LoamTool
import loamstream.tools.klusta.KlustaKwikKonfig
import loamstream.util.FileAsker

import scala.language.implicitConversions

/**
  * LoamStream
  * Created by oliverr on 4/1/2016.
  */
object LCoreEnv {

  object Keys {
    val vcfFilePath = Key.create[String => Path]
    val sampleFilePath = Key.create[() => Path]
    val singletonFilePath = Key.create[() => Path]
    val pcaWeightsFilePath = Key.create[() => Path]
    val klustaKwikKonfig = Key.create[KlustaKwikKonfig]
    val pcaProjectionsFilePath = Key.create[() => Path]
    val clusterFilePath = Key.create[() => Path]
    val genotypesId = Key.create[String]
    val vdsId = Key.create[String]
    val singletonsId = Key.create[String]
    val pcaWeightsId = Key.create[String]
    val command = LEnv.Key.create[LoamTool]
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
