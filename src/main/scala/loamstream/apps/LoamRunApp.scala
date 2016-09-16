package loamstream.apps

import loamstream.compiler.messages.ClientMessageHandler.OutMessageSink.LoggableOutMessageSink
import loamstream.compiler.{ LoamCompiler, LoamEngine }
import loamstream.util.Loggable
import loamstream.model.execute.NaiveFilteringExecuter
import loamstream.db.slick.SlickLoamDao
import loamstream.db.slick.DbType
import loamstream.db.LoamDao
import loamstream.model.execute.JobFilter
import loamstream.db.slick.DbDescriptor

/** Compiles and runs Loam script provided as argument */
object LoamRunApp extends App with Loggable {
  if (args.length < 1) {
    throw new IllegalArgumentException("No Loam script file name provided")
  }

  if (args.length > 1) {
    throw new IllegalArgumentException("This app takes only one argument, the Loam script file name.")
  }

  import scala.concurrent.ExecutionContext.Implicits.global

  //TODO: Make this configurable
  val dbDescriptor = DbDescriptor(DbType.H2, s"jdbc:h2:./.loamstream/db;DB_CLOSE_DELAY=-1")
  
  withDao(new SlickLoamDao(dbDescriptor)) { dao =>
  
    val executer = new NaiveFilteringExecuter(new JobFilter.DbBackedJobFilter(dao))
  
    val outMessageSink = LoggableOutMessageSink(this)
  
    val loamEngine = LoamEngine(new LoamCompiler(outMessageSink), executer, outMessageSink)
    val engineResult = loamEngine.runFile(args(0))
  
    for {
      (job, result) <- engineResult.jobResultsOpt.get
    } {
      info(s"Got $result when running $job")
    }
  }
  
  private def withDao[A](dao: LoamDao)(f: LoamDao => A): A = {
    try { 
      //TODO: Make whether this happens configurable
      dao.createTables() 
      
      f(dao) 
    } finally { 
      dao.shutdown() 
    }
  }
}
