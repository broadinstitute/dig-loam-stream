package loamstream.model

import org.scalatest.FunSuite
import loamstream.TestHelpers
import loamstream.loam.LoamScriptContext
import loamstream.loam.LoamGraph.StoreLocation
import loamstream.util.BashScript
import java.net.URI

/**
 * @author clint
 * May 2, 2018
 */
final class StoreTest extends FunSuite {

  private def testWithScriptContext(name: String)(body: LoamScriptContext => Any): Unit = {
    test(name) {
      TestHelpers.withScriptContext(body)
    }
  }
  
  testWithScriptContext("hashCode") { implicit context =>
    val s0 = Store()
    val s1 = Store()
    
    assert(s0.id !== s1.id)
    assert(s0.path !== s1.path)
    
    assert(s0.hashCode !== s1.hashCode)
    
    val sameIdAsS0ButDifferentLocation = Store(s0.id, StoreLocation.PathLocation(s1.path))
    
    assert(s0.hashCode === sameIdAsS0ButDifferentLocation.hashCode)
    
    assert(sameIdAsS0ButDifferentLocation.hashCode !== s1.hashCode)
  }
  
  testWithScriptContext("equals") { implicit context =>
    val s0 = Store()
    val s1 = Store()
    
    assert(s0.id !== s1.id)
    assert(s0.path !== s1.path)
    
    assert(s0 === s0)
    assert(s1 === s1)
    
    assert(s0 !== s1)
    assert(s1 !== s0)
    
    val sameIdAsS0ButDifferentLocation = Store(s0.id, StoreLocation.PathLocation(s1.path))
    
    assert(s0 === sameIdAsS0ButDifferentLocation)
    assert(sameIdAsS0ButDifferentLocation === s0)
    
    assert(s1 !== sameIdAsS0ButDifferentLocation)
    assert(sameIdAsS0ButDifferentLocation !== s1)
  }
  
  testWithScriptContext("toString") { implicit context =>

    import BashScript.Implicits._
    
    val id = LId.newAnonId
    
    {
      val p = TestHelpers.path("/foo/bar")
      
      val pathStore = Store(id, StoreLocation.PathLocation(p)) 
      
      val expected = s"store(${id})@${p.render}"
      
      assert(pathStore.toString === expected)
    }
    
    {
      val u = URI.create("gs://loamstream/foo/bar")
      
      val uriStore = Store(id, StoreLocation.UriLocation(u)) 
      
      val expected = s"store(${id})@${u}"
      
      assert(uriStore.toString === expected)
    }
  }
  
  testWithScriptContext("render") { implicit context =>
    val p = TestHelpers.path("/foo/bar")
      
    val pathStore = Store(StoreLocation.PathLocation(p))
    
    val u = URI.create("gs://loamstream/foo/bar")
      
    val uriStore = Store(StoreLocation.UriLocation(u))
    
    import BashScript.Implicits._
    
    assert(pathStore.render === p.render)
    assert(uriStore.render === u.toString)
  }
  
  testWithScriptContext("pathOpt/uriOpt/path/get") { implicit context =>
    val id = LId.newAnonId
    
    val p = TestHelpers.path("/foo/bar")
      
    val pathStore = Store(id, StoreLocation.PathLocation(p))
    
    val u = URI.create("gs://loamstream/foo/bar")
      
    val uriStore = Store(id, StoreLocation.UriLocation(u)) 
    
    assert(pathStore.pathOpt === Some(p))
    assert(uriStore.pathOpt === None)
    
    assert(pathStore.uriOpt === None)
    assert(uriStore.uriOpt === Some(u))
    
    assert(pathStore.path === p)
    
    intercept[Exception] {
      uriStore.path
    }
    
    intercept[Exception] {
      pathStore.uri
    }
    
    assert(uriStore.uri === u)
  }
  
  testWithScriptContext("apply") { implicit context =>
    //Anon id, anon (path) location:
    {
      val s0 = Store()
      
      assert(s0.id.isInstanceOf[LId.LAnonId])
      assert(s0.uriOpt === None)
      assert(s0.pathOpt.isDefined)
      
      val s1 = Store()
      
      assert(s0.id !== s1.id)
      assert(s0.path !== s1.path)
    }
    
    import TestHelpers.path
    
    val p = path("/foo/bar")
    val u = URI.create("gs://loamstream/foo/bar")
    val id = LId.newAnonId
    
    //Anon id, path location 
    {
      val pathStore = Store(StoreLocation.PathLocation(p))
      
      assert(pathStore.id.isInstanceOf[LId.LAnonId])
      assert(pathStore.uriOpt === None)
      assert(pathStore.pathOpt === Some(p))
    }
    
    //Anon id, URI location 
    {
      val uriStore = Store(StoreLocation.UriLocation(u))
      
      assert(uriStore.id.isInstanceOf[LId.LAnonId])
      assert(uriStore.uriOpt === Some(u))
      assert(uriStore.pathOpt === None)
    }
    
    //Explicit id, path location 
    {
      val pathStore = Store(id, StoreLocation.PathLocation(p))
      
      assert(pathStore.id === id)
      assert(pathStore.uriOpt === None)
      assert(pathStore.pathOpt === Some(p))
    }
    
    //Explicit id, URI location 
    {
      val uriStore = Store(id, StoreLocation.UriLocation(u))
      
      assert(uriStore.id === id)
      assert(uriStore.uriOpt === Some(u))
      assert(uriStore.pathOpt === None)
    }
  }
}
