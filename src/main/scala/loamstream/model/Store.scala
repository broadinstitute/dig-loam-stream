package loamstream.model

import java.net.URI
import java.nio.file.{Path, Paths}

import loamstream.loam.LoamGraph.StoreLocation
import loamstream.loam.{LoamGraph, LoamProjectContext, LoamScriptContext, LoamStoreKeySlot, LoamStoreRef}
import loamstream.loam.ops.StoreType.TXT
import loamstream.loam.ops.{StoreField, StoreType, TextStore, TextStoreField}
import loamstream.util.TypeBox

import scala.reflect.runtime.universe.TypeTag
import loamstream.loam.HasLocation

/**
  * LoamStream
  * Created by oliverr on 6/8/2016.
  */
object Store {

  trait Untyped extends HasLocation {
    def id: LId

    def sig: TypeBox.Untyped

    def scriptContext: LoamScriptContext

    def projectContext: LoamProjectContext = scriptContext.projectContext

    def update(): Unit = projectContext.updateGraph(_.withStore(this))

    def asInput: Store.Untyped

    def at(path: String): Store.Untyped

    def at(path: Path): Store.Untyped

    def at(uri: URI): Store.Untyped

    def at(location: StoreLocation): Store.Untyped

    def key(name: String): LoamStoreKeySlot = LoamStoreKeySlot(this, name)(projectContext)

    override def toString: String = {
      val simpleTypeName = sig.tpe.toString.split("\\.").lastOption.getOrElse("?")
      
      val location = (pathOpt orElse uriOpt).map(_.toString).getOrElse(path)
      
      s"store[${simpleTypeName}]($id)@$location"
    }
    
    def graph: LoamGraph = projectContext.graph

    override def pathOpt: Option[Path] = graph.pathOpt(this)

    override def path: Path = projectContext.fileManager.getPath(this)

    override def uriOpt: Option[URI] = graph.uriOpt(this)

    def +(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixAdder(suffix))

    def -(suffix: String): LoamStoreRef = LoamStoreRef(this, LoamStoreRef.suffixRemover(suffix))
  }

  def create[S <: StoreType : TypeTag](implicit scriptContext: LoamScriptContext): Store[S] = {
    Store[S](LId.newAnonId)
  }
}

final case class Store[S <: StoreType : TypeTag] private(id: LId)(implicit val scriptContext: LoamScriptContext)
  extends Store.Untyped {

  override val sig: TypeBox[S] = TypeBox.of[S]

  update()

  override def asInput: Store[S] = {
    projectContext.updateGraph(_.withStoreAsInput(this))
    
    this
  }

  override def at(path: String): Store[S] = at(Paths.get(path))

  override def at(path: Path): Store[S] = {
    val resolvedPath = scriptContext.workDir.resolve(path)
    val location = StoreLocation.PathLocation(resolvedPath)
    at(location)
  }

  override def at(uri: URI): Store[S] = {
    val location = StoreLocation.UriLocation(uri)
    at(location)
  }

  override def at(location: StoreLocation): Store[S] = {
    projectContext.updateGraph(_.withStoreLocation(this, location))
    
    this
  }
}



