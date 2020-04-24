package loamstream.db.slick.migration

import scala.util.Try
import loamstream.db.slick.CommonDaoOps
import loamstream.db.slick.DbDescriptor
import loamstream.loam.LoamGraph
import loamstream.compiler.LoamEngine
import loamstream.model.execute.Executable
import loamstream.db.LoamDao
import loamstream.db.slick.OutputDaoOps
import loamstream.db.slick.ExecutionDaoOps
import loamstream.db.slick.CommandDaoOps
import loamstream.model.jobs.LJob
import loamstream.db.slick.OutputRow

/**
 * @author clint
 * Apr 22, 2020
 */
trait Migration extends CommonDaoOps {
  def migrate(): Try[Unit] 
}
