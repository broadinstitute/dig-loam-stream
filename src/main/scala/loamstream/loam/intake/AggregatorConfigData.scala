package loamstream.loam.intake

import java.nio.file.Path

/**
 * @author clint
 * Feb 12, 2020
 */
final case class AggregatorConfigData(metadata: AggregatorMetadata, columns: SourceColumns, csvFile: Path) {
  def asConfigFileContents: String = {
    s"""|${metadata.asConfigFileContents}
        |
        |${columns.asConfigFileContents}
        |
        |load ${csvFile}
        |
        |process
        |
        |quit""".stripMargin
  }
}
