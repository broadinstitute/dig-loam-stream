package loamstream.loam.intake.dga

import loamstream.loam.intake.DataRow
import loamstream.util.HttpClient
import loamstream.util.SttpHttpClient
import org.json4s._
import org.json4s.JsonAST.JNumber
import loamstream.loam.intake.Source
import loamstream.loam.intake.ColumnName
import loamstream.util.TimeUtils
import loamstream.util.Loggable

/**
 * @author clint
 * Dec 1, 2020
 */
trait DgaSyntax { 
  object Dga extends TissueSupport with AnnotationsSupport with BedSupport with Loggable
}
