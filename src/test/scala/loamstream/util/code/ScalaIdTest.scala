package loamstream.util.code

import org.scalatest.FunSuite

/**
  * LoamStream
  * Created by oliverr on 9/16/2016.
  */
final class ScalaIdTest extends FunSuite {
  test("PackageId") {
    assert(RootPackageId.parentOpt === None)
    val a = PackageId("a")
    assert(a.parent === RootPackageId)
  }

}
