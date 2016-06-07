package loamstream.model

import java.nio.file.Path

import loamstream.Sigs

import scala.reflect.runtime.universe.typeOf

/**
  * @author clint
  *         date: May 25, 2016
  */
final case class FileStore(path: Path, spec: StoreSpec) extends Store {
  //TODO: is this right?
  override val id: LId = LId.LNamedId(path.toAbsolutePath.toString)
}

object FileStore {

  import Sigs._

  def vcfFile(path: Path): FileStore = FileStore(path, StoreSpec(variantAndSampleToGenotype))

  def vdsFile(path: Path): FileStore = FileStore(path, StoreSpec(variantAndSampleToGenotype))

  def pcaWeightsFile(path: Path): FileStore = FileStore(path, StoreSpec(sampleIdAndIntToDouble))

  def pcaProjectedFile(path: Path): FileStore = FileStore(path, StoreSpec(sampleIdAndIntToDouble))

  def sampleClusterFile(path: Path): FileStore = FileStore(path, StoreSpec(LSig(typeOf[Map[String, Int]])))

  def singletonsFile(path: Path): FileStore = FileStore(path, StoreSpec(sampleToSingletonCount))

  def sampleIdsFile(path: Path): FileStore = FileStore(path, StoreSpec(Sigs.sampleIds))
}