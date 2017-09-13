package loamstream.wdl4s

import wdl4s.{WdlNamespace, WdlNamespaceWithWorkflow}

/**
  * LoamStream
  * Created by oliverr on 6/12/2017.
  */
object Wdl4sInspecter {

  def indent(string: String): String = string.split("\n").map("  " + _).mkString("\n", "\n", "\n")

  def inspectAnyRef(anyRef: AnyRef): String = anyRef.getClass.getCanonicalName + "@" + anyRef.hashCode()

  def inspectNamespaceWithWorkflow(ns: WdlNamespaceWithWorkflow): String = {
    val lines = Seq(
      "class: " + ns.getClass.getCanonicalName,
      "importedAs: " + ns.importedAs,
      "workflow: " + ns.workflow,
      "imports: " + ns.imports,
      "namespaces: " + ns.namespaces,
      "tasks: " + ns.tasks,
//      "terminalMap: " + ns.terminalMap,
//      "wdlSyntaxErrorFormatter: " + ns.wdlSyntaxErrorFormatter,
    "ast: " + ns.ast
    )
    lines.mkString("\n")
  }

  def inspectNameSpace(ns: WdlNamespace): String = ns match {
    case nsWithWorkflow: WdlNamespaceWithWorkflow => inspectNamespaceWithWorkflow(nsWithWorkflow)
    case _ => ???
  }

  def inspect(item: AnyRef): String = item match {
    case nameSpace: WdlNamespace => inspectNameSpace(nameSpace)
    case anyRef => inspectAnyRef(anyRef)
  }

}
