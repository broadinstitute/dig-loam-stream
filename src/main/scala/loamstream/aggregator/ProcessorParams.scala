package loamstream.aggregator

import java.net.URI

final case class ProcessorParams(name: String, resources: Map[String, URI], outputs: Seq[String])
