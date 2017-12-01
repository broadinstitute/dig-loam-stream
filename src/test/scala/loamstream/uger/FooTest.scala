package loamstream.uger

import org.scalatest.FunSuite
import rx.lang.scala.Observable
import loamstream.util.ObservableEnrichments
import scala.concurrent.Await
import loamstream.util.Observables
import rx.lang.scala.Subject
import rx.lang.scala.subjects.PublishSubject
import rx.lang.scala.subjects.ReplaySubject
import loamstream.model.jobs.JobStatus
import ObservableEnrichments._
import scala.concurrent.duration._
import sun.security.ssl.HandshakeMessage.Finished

final class FooTest extends FunSuite {
  import FooTest._
  
  test("bar") {
    /*
     * gc0
     *    \
     *     +-c0-+
     *    /      \
     * gc1        \
     *             +-root
     * gc2        /
     *    \      /
     *     +-c1-+
     *    /
     * gc3
     * 
     */
    val gc0 = Node("GC_0")
    val gc1 = Node("GC_1")
    val gc2 = Node("GC_2")
    val gc3 = Node("GC_3")
    
    val c0 = Node("C_0", Seq(gc0, gc1))
    val c1 = Node("C_1", Seq(gc2, gc3))
    
    val root = Node("R", Seq(c0, c1))
    
    val allNodes = Seq(gc0, gc1, gc2, gc3, c0, c1, root)
    
    allNodes.foreach(statusUpdateThread)
    
    val f = root.runnables.take(7).toSeq.firstAsFuture
    
    val ran = Await.result(f, Duration.Inf)
    
    assert(ran.toSet === allNodes.toSet) 
  }
  
  test("baz") {
    /*
     * gc0
     *    \
     *     +-c0-+
     *    /      \
     * gc1        \
     *             +-root
     * gc2        /
     *    \      /
     *     +-c1-+
     *    /
     * gc3
     * 
     */
    
    val branchingFactor = 10
    
    def grandChildren(cid: Int): Seq[Node] = (0 until branchingFactor).map { 
      i => Node(s"GC_${cid}_${i}")
    }
    
    def children: Seq[Node] = (0 until branchingFactor).map { i => 
      Node(s"C_${i}", grandChildren(i))
    }
    
    val root = Node("R", children)
    
    def allNodesFrom(n: Node): Seq[Node] = {
      n +: n.inputs.flatMap(allNodesFrom)
    }
    
    val allNodes: Seq[Node] = allNodesFrom(root) 
    
    val expectedNumNodes = (branchingFactor * branchingFactor) + branchingFactor + 1
    
    assert(allNodes.size === expectedNumNodes)
    
    allNodes.foreach(statusUpdateThread)
    
    val f = root.runnables.take(expectedNumNodes).toSeq.firstAsFuture
    
    val ran = Await.result(f, Duration.Inf)
    
    assert(ran.toSet === allNodes.toSet) 
  }
  
  test("blerg") {
    /*
     * gc0
     *    \
     *     +-c0-+
     *    /      \
     * gc1        \
     *             +-root
     * gc2        /
     *    \      /
     *     +-c1-+
     *    /
     * gc3
     * 
     */
    
    val branchingFactor = 10
    
    val grandChild: Node = Node(s"GC")
    
    def children: Seq[Node] = (0 until branchingFactor).map { i => 
      Node(s"C_${i}"/*, grandChildren(i)*/)
    }
    
    val root = Node("R", children)
    
    def allNodesFrom(n: Node): Seq[Node] = {
      n +: n.inputs.flatMap(allNodesFrom)
    }
    
    val allNodes: Seq[Node] = allNodesFrom(root) 
    
    val expectedNumNodes = (branchingFactor * branchingFactor) + branchingFactor + 1
    
    assert(allNodes.size === expectedNumNodes)
    
    allNodes.foreach(statusUpdateThread)
    
    val f = root.runnables.take(expectedNumNodes).toSeq.firstAsFuture
    
    val ran = Await.result(f, Duration.Inf)
    
    assert(ran.toSet === allNodes.toSet) 
  }
  
  ignore("foo") {
    val n = 10000
    
    val is: IndexedSeq[Int] = (1 to n).toIndexedSeq
    
    //val isObservable = Observable.from(is)
    
    val subject: Subject[Int] = PublishSubject()//ReplaySubject()
    
    val t = new Thread(new Runnable {
      private var i = 0
      
      override def run(): Unit = {
        var i = 0
        while(i < n) {
          subject.onNext(is(i))
          
          i += 1
          
          Thread.sleep(scala.util.Random.nextInt(20))
        }
        subject.onCompleted()
      }
    })
    
    t.setDaemon(true)
    
    val isObservable: Observable[Int] = subject
    
    
    
    val chunks = isObservable.tumbling(100.milliseconds, 7)
    
    val z: Observable[Seq[Int]] = Observable.just(Vector.empty)
    
    val seqs = chunks.map(_.to[Seq]).flatMap(identity)
    
    val reducedObs = seqs.foldLeft(Seq.empty[Int]) { (acc, chunk) =>
      println(s"chunk: $chunk")
      
      acc ++ chunk
    }
    
    t.start()
    
    /*val reducedObs = chunks.map(_.to[Seq]).reduce { (accObs, chunkObs) =>
      for {
        acc <- accObs
        chunk <- chunkObs
        _ = println(s"chunk: $chunk")
      } yield acc ++ chunk
    }.flatten*/
    
    val tumbled = Await.result(reducedObs.lastAsFuture, Duration.Inf)
    
    assert(is.size === tumbled.size)
    
    assert(is.toSet === tumbled.toSet)
  }
}

object FooTest {
  def statusUpdateThread(node: Node): Thread = {
    import JobStatus._
    
    val statuses: Seq[JobStatus] = Seq(NotStarted, Submitted, Running, Running, Succeeded)
    
    val t: Thread = new Thread(new Runnable {
      override def run(): Unit = {
        statuses.foreach { status =>
          
          node.transitionTo(status)
          
          Thread.sleep(scala.util.Random.nextInt(20))
        }
      }
    })
    
    t.setDaemon(true)
    
    node.selfRunnables.take(1).foreach(_ => t.start())
    
    t
  }
  
  final case class State(id: String, status: JobStatus)
  
  val ids: Iterator[String] = {
    for { 
      i <- (1 to 100).iterator 
      ch <- "ABCDEFGHIJKLMNOPQRSTUV".iterator 
    } yield { ch.toString * i }
  }
  
  final case class Node(id: String = ids.next(), inputs: Seq[Node] = Nil) {
    private val subject: Subject[JobStatus] = ReplaySubject()
    
    val states: Observable[State] = subject.map(status => State(id, status)).share
    
    val finalState: Observable[State] = states.filter(_.status.isTerminal).take(1).share
    
    val finalDepStates: Observable[Seq[State]] = Observables.sequence(inputs.map(_.finalState)).share
    
    val selfRunnables: Observable[Node] = {
      def justUs: Observable[Node] = Observable.just(this)
      
      def noMore: Observable[Node] = Observable.empty
      
      val result = {
        if(inputs.isEmpty) { justUs }
        else {
          for {
            depStates <- finalDepStates
            anyDepFailures = depStates.exists(_.status.isFailure)
            runnable <- if(anyDepFailures) noMore else justUs
          } yield runnable
        }
      }
      
      result.share
    }
    
    val runnables: Observable[Node] = {
      //Multiplex the streams of runnable jobs starting from each of our dependencies
      val dependencyRunnables = {
        if(inputs.isEmpty) { Observable.empty }
        //NB: Note the use of merge instead of ++; this ensures that we don't emit jobs from the sub-graph rooted at
        //one dependency before the other dependencies, but rather emit all the streams of runnable jobs "together".
        else { Observables.merge(inputs.map(_.runnables)) }
      }

      //Emit the current job *after* all our dependencies
      //NB: Note use of .share which allows re-using this Observable, saving much memory when running complex pipelines
      (dependencyRunnables ++ selfRunnables).share/*.replay.refCount*/
    }
    
    
    def transitionTo(status: JobStatus): Unit = {
      subject.onNext(status)
      
      if(status.isTerminal) {
        subject.onCompleted()
      }
    }
  }
}
