package loamstream.loam.intake

import org.scalatest.FunSuite

/**
 * @author clint
 * Feb 10, 2020
 */
final class ColumnExprTest extends FunSuite {
  private val nullRow: CsvRow = null //scalastyle:ignore
  
  private val foo = ColumnName("foo")
  private val bar = ColumnName("bar")
  private val baz = ColumnName("baz")
  
  test("render") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "42", "baz" -> "x_y_z_q")
    
    assert(baz.render(row) === "x_y_z_q")
    
    assert(foo.asDouble.map(_ * 2.0).render(row) === "0.2")
  }
  
  test("map") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "42", "baz" -> "x_y_z_q")
    
    val f: Int => Double = _.toDouble
    
    assert(bar.asInt.map(f).apply(row) === 42.0D)
  }
  
  test("flatMap") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "42", "baz" -> "x_y_z_q")
    
    val f: (Int => ColumnExpr[Double]) = i => LiteralColumnExpr(i.toDouble)
    
    assert(bar.asInt.flatMap(f).apply(row) === 42.0D)
  }
  
  test("asString") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "42", "baz" -> "x_y_z_q")
    
    assert(foo.asString(row) === "0.1")
    assert(bar.asInt.map(_ + 1).asString(row) === "43")
  }
  
  test("as{Int,Long,Double}") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "42", "baz" -> "x_y_z_q")
    
    intercept[Exception] {
      foo.asInt.apply(row)
    }
    
    assert(bar.asInt.apply(row) === 42)
    
    intercept[Exception] {
      foo.asLong.apply(row)
    }
    
    assert(bar.asLong.apply(row) === 42L)
    
    assert(foo.asDouble.apply(row) === 0.1D)
    assert(bar.asDouble.apply(row) === 42.0D)
  }
  
  test("asUpperCase") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "hello99", "baz" -> "x_y_z_q")
    
    assert(foo.asUpperCase.apply(row) === "0.1")
    assert(bar.asUpperCase.apply(row) === "HELLO99")
  }
  
  test("unary_-") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "hello99", "baz" -> "x_y_z_q")
    
    assert(-(foo.asDouble).apply(row) === -0.1)
    assert(-(-(foo.asDouble)).apply(row) === 0.1)
    assert(-(-(-(foo.asDouble))).apply(row) === -0.1)
  }
  
  test("negate") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "hello99", "baz" -> "x_y_z_q")
    
    assert(foo.asDouble.negate.apply(row) === -0.1)
    assert(foo.asDouble.negate.negate.apply(row) === 0.1)
    assert(foo.asDouble.negate.negate.negate.apply(row) === -0.1)
  }
  
  test("complement/complementIf") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "hello99", "baz" -> "x_y_z_q")
    
    assert(foo.asDouble.complement.apply(row) === 0.9)
    
    assert(foo.asDouble.complementIf(_ > 0.5).apply(row) === 0.1)
    assert(foo.asDouble.complementIf(_ < 0.5).apply(row) === 0.9)
  }
  
  test("exp") {
    val row = Helpers.csvRow("foo" -> "0.1", "bar" -> "hello99", "baz" -> "x_y_z_q")
    
    assert(foo.asDouble.exp.apply(row) === scala.math.exp(0.1))
  }
  
  test("mapRegex") {
    val fourParts = "(.+)_(.+)_(.+)_(.+)".r
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "hello99", "baz" -> "x_y_z_q")
    val noMatch = Helpers.csvRow("foo" -> "42", "bar" -> "hello99", "baz" -> "asdf")
    
    val expr: ColumnExpr[String] = baz.mapRegex(fourParts) {
      case Seq(a, b, c, d) => a + b + c + d
    }
    
    assert(expr(row) === "xyzq")
    
    intercept[Exception] {
      expr(noMatch)
    }
  }
  
  test("orElse") {
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "hello99", "baz" -> "hello")
    
    val someExpr: ColumnExpr[Option[Char]] = bar.map(_.headOption)
    
    assert(someExpr.orElse('z').apply(row) === 'h')
    
    val noneExpr: ColumnExpr[Option[Char]] = foo.map(_ => None)
    
    assert(noneExpr.orElse('z').apply(row) === 'z') 
  }
  
  test("matches") {
    val justNumbers = "\\d+"
    val helloNumbers = "hello\\d+".r
    
    val shouldMatch = Helpers.csvRow("foo" -> "42", "bar" -> "hello99", "baz" -> "hello")
    val shouldNotMatch = Helpers.csvRow("foo" -> "asdf", "bar" -> "hello", "baz" -> "hello")
    
    val fooIsJustNumbers = foo.matches(justNumbers)
    val barIsHelloNumbers = bar.matches(helloNumbers)
    
    assert(fooIsJustNumbers(shouldMatch) === true)
    assert(barIsHelloNumbers(shouldMatch) === true)
    
    assert(fooIsJustNumbers(shouldNotMatch) === false)
    assert(barIsHelloNumbers(shouldNotMatch) === false)
  }
  
  test("===/!==") {
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "99", "baz" -> "hello")
    
    val fooIs42: RowPredicate = foo.asInt === 42
    val fooIsNot42: RowPredicate = foo.asInt !== 42
    
    val barIs42: RowPredicate = bar.asInt === 42
    val barIsNot42: RowPredicate = bar.asInt !== 42
    
    assert(fooIs42(row) === true)
    assert(fooIsNot42(row) === false)
    
    assert(barIs42(row) === false)
    assert(barIsNot42(row) === true)
  }
  
  test(">, >=, <, <=") {
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "99", "baz" -> "hello")
    
    val fooLt42: RowPredicate = foo.asInt < 42
    val fooLt99: RowPredicate = foo.asInt < 99
    val fooLtE42: RowPredicate = foo.asInt <= 42
    val fooLtE99: RowPredicate = foo.asInt <= 99
    
    val barGt42: RowPredicate = bar.asInt > 42
    val barGtE99: RowPredicate = bar.asInt >= 99
    val barGtE42: RowPredicate = bar.asInt >= 42
    
    
    assert(fooLt42(row) === false)
    assert(fooLtE42(row) === true)
    assert(fooLt99(row) === true)
    assert(fooLtE99(row) === true)
    
    assert(barGt42(row) === true)
    assert(barGtE99(row) === true)
    assert(barGtE42(row) === true)
  }
  
  test("fromRowParser") {
    val rowParser: RowParser[Long] = _ => 42L
    
    val expr = ColumnExpr.fromRowParser(rowParser)
    
    assert(expr(nullRow) === 42L)
  }
  
  test("ExprOps / {+,-,*,/}") {
    import ColumnExpr.ExprOps
    //ints
    {
      val lhs: Int = 42
      
      val plus1 = lhs + LiteralColumnExpr(1)
      val minus1 = lhs - LiteralColumnExpr(1)
      val times2 = lhs * LiteralColumnExpr(2)
      
      assert(plus1(nullRow) === 43)
      assert(minus1(nullRow) === 41)
      assert(times2(nullRow) === 84)
    }
    //longs
    {
      val lhs: Long = 42L
      
      val plus1 = lhs + LiteralColumnExpr(1L)
      val minus1 = lhs - LiteralColumnExpr(1L)
      val times2 = lhs * LiteralColumnExpr(2L)
      
      assert(plus1(nullRow) === 43L)
      assert(minus1(nullRow) === 41L)
      assert(times2(nullRow) === 84L)
    }
    //floats
    {
      val lhs: Float = 42F
      
      val plus1 = lhs + LiteralColumnExpr(1F)
      val minus1 = lhs - LiteralColumnExpr(1F)
      val times2 = lhs * LiteralColumnExpr(2F)
      val divBy2 = lhs / LiteralColumnExpr(2F)
      
      assert(plus1(nullRow) === 43F)
      assert(minus1(nullRow) === 41F)
      assert(times2(nullRow) === 84F)
      assert(divBy2(nullRow) === 21F)
    }
    //doubles
    {
      val lhs: Double = 42D
      
      val plus1 = lhs + LiteralColumnExpr(1D)
      val minus1 = lhs - LiteralColumnExpr(1D)
      val times2 = lhs * LiteralColumnExpr(2D)
      val divBy2 = lhs / LiteralColumnExpr(2D)
      
      assert(plus1(nullRow) === 43D)
      assert(minus1(nullRow) === 41D)
      assert(times2(nullRow) === 84D)
      assert(divBy2(nullRow) === 21D)
    }
  }
  
  test("ExprOps / {+,-,*,/} - LHS is expr, RHS is literal") {
    import ColumnExpr.ExprOps
    //ints
    {
      val lhs = LiteralColumnExpr(42)
      
      val plus1 = lhs + 1
      val minus1 = lhs - 1
      val times2 = lhs * 2
      
      assert(plus1(nullRow) === 43)
      assert(minus1(nullRow) === 41)
      assert(times2(nullRow) === 84)
    }
    //longs
    {
      val lhs = LiteralColumnExpr(42L)
      
      val plus1 = lhs + 1L
      val minus1 = lhs - 1L
      val times2 = lhs * 2L
      
      assert(plus1(nullRow) === 43L)
      assert(minus1(nullRow) === 41L)
      assert(times2(nullRow) === 84L)
    }
    //floats
    {
      val lhs = LiteralColumnExpr(42F)
      
      val plus1 = lhs + 1F
      val minus1 = lhs - 1F
      val times2 = lhs * 2F
      val divBy2 = lhs / 2F
      
      assert(plus1(nullRow) === 43F)
      assert(minus1(nullRow) === 41F)
      assert(times2(nullRow) === 84F)
      assert(divBy2(nullRow) === 21F)
    }
    //doubles
    {
      val lhs = LiteralColumnExpr(42D)
      
      val plus1 = lhs + 1D
      val minus1 = lhs - 1D
      val times2 = lhs * 2D
      val divBy2 = lhs / 2D
      
      assert(plus1(nullRow) === 43D)
      assert(minus1(nullRow) === 41D)
      assert(times2(nullRow) === 84D)
      assert(divBy2(nullRow) === 21D)
    }
  }
  
  test("StringsAreConvertableToNumbers") {
    import ColumnExpr.StringsAreConvertableToNumbers.{toInt, toLong, toFloat, toDouble}
    
    assert(toInt("42") === 42)
    assert(toLong("42") === 42L)
    assert(toFloat("42") === 42.0F)
    assert(toDouble("42") === 42.0D)
    
    intercept[Exception] {
      toInt("asdf")
    }
    
    intercept[Exception] {
      toLong("asdf")
    }
    
    intercept[Exception] {
      toFloat("asdf")
    }
    
    intercept[Exception] {
      toDouble("asdf")
    }
  }
  
  test("NumericTypesAreConvertableToNumbers") {
    import ColumnExpr.numericTypesAreConvertableToNumbers
    
    val intsAreConvertible = numericTypesAreConvertableToNumbers[Int] 
      
    import intsAreConvertible.{toInt, toLong, toFloat, toDouble}
    
    assert(toInt(42) === 42)
    assert(toLong(42) === 42L)
    assert(toFloat(42) === 42.0F)
    assert(toDouble(42) === 42.0D)
  }

  test("lift") {
    val f: Int => Double = _.toDouble
    
    val lifted = ColumnExpr.lift(f)
    
    val row = Helpers.csvRow("foo" -> "99", "bar" -> "42") 
    
    val intExpr = ColumnName("bar").asInt
    
    val doubleExpr = lifted(intExpr)
    
    assert(doubleExpr(row) === 42.0)
  }
  
  test("lift2") {
    val f: (Int, Int) => Double = (a, b) => a.toDouble / b.toDouble
    
    val lifted = ColumnExpr.lift2(f)
    
    val row = Helpers.csvRow("foo" -> "42", "bar" -> "2") 
    
    val aExpr = ColumnName("foo").asInt
    val bExpr = ColumnName("bar").asInt
    
    val doubleExpr = lifted(aExpr, bExpr)
    
    assert(doubleExpr(row) === 21.0)
  }
  
  test("PartialColumnExpr") {
    val pexpr = ColumnExpr.fromPartialRowParser[Int]({
      case row if row.getFieldByIndex(0) == "foo" => row.getFieldByIndex(1).toInt
    })
    
    val rowInDomain = Helpers.csvRow("bar" -> "foo", "baz" -> "42", "foo" -> "lol")
    val rowNotInDomain = Helpers.csvRow("bar" -> "glerg", "baz" -> "42", "foo" -> "lol")
    
    assert(pexpr.isDefinedAt(rowInDomain) === true)
    assert(pexpr.isDefinedAt(rowNotInDomain) === false)
    
    assert(pexpr(rowInDomain) === 42)
    assert(pexpr(rowInDomain) === 42)
  }
  
  test("LiteralColumnExpr") {
    val a = LiteralColumnExpr(42)

    assert(a.value === 42)
    assert(a.asString(nullRow) === "42")
    assert(a(nullRow) === 42)
    assert(a.toString === "42")
    
    val b = LiteralColumnExpr("hello")
    
    assert(b.value === "hello")
    assert(b.asString(nullRow) === "hello")
    assert(b(nullRow) === "hello")
    assert(b.toString === "hello")
  }
  
  test("ColumnName") {
    val cn = ColumnName("foo")

    val row = Helpers.csvRow("bar" -> "42", "baz" -> "asdf", "foo" -> "lol")
    
    assert(cn.asString(row) === "lol")
    assert(cn(row) === "lol")
  }
  
  test("trim") {
    val cn = ColumnName("baz")

    val row = Helpers.csvRow("bar" -> "42", "baz" -> "asdf  ", "foo" -> "lol")
    
    assert(cn(row) === "asdf  ")
    assert(cn.trim.apply(row) === "asdf")
  }
  
  test("isEmpty") {
    val baz = ColumnName("baz")
    val foo = ColumnName("foo")
    val blerg = ColumnName("blerg")

    val row = Helpers.csvRow("bar" -> "42", "baz" -> "asdf  ", "foo" -> "", "blerg" -> "  ")
    
    assert(baz.isEmpty.apply(row) === false)
    assert(foo.isEmpty.apply(row) === true)
    assert(blerg.isEmpty.apply(row) === false)
    
  }
  
  test("isEmptyIgnoreWhitespace") {
    val baz = ColumnName("baz")
    val foo = ColumnName("foo")
    val blerg = ColumnName("blerg")

    val row = Helpers.csvRow("bar" -> "42", "baz" -> "asdf  ", "foo" -> "", "blerg" -> "  ")
    
    assert(baz.isEmptyIgnoreWhitespace.apply(row) === false)
    assert(foo.isEmptyIgnoreWhitespace.apply(row) === true)
    assert(blerg.isEmptyIgnoreWhitespace.apply(row) === true)
  }
  
  test("mapName") {
    val baz = ColumnName("baz")
    
    val BAZ = baz.mapName(_.toUpperCase)
    
    val row = Helpers.csvRow("bar" -> "42", "baz" -> "asdf  ", "foo" -> "", "blerg" -> "  ")
    
    assert(baz.name.toUpperCase === BAZ.name)
    
    assert(baz.eval(row) === "asdf  ")
    
    intercept[Exception] {
      BAZ.eval(row)
    }
  }
}
