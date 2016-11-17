package loamstream.model.jobs.commandline

import org.scalatest.FunSuite

/**
 * @author clint
 * Nov 16, 2016
 */
final class CommandLineStringJobTest extends FunSuite {
  test("escapeCommandString") {
    import CommandLineStringJob.escapeCommandString
    
    assert(escapeCommandString("") === "")
    assert(escapeCommandString("abc") === "abc")
    assert(escapeCommandString(" foo ") === " foo ")
    
    assert(escapeCommandString("$") === """\\\\$""")
    
    assert(escapeCommandString("""\""") === """\\\\""")

    //scalastyle:off line.length
    val complex = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
    
    val expected = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if(\\\\$8 >= 0.0884) print \\\\$0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
    //scalastyle:off line.length
    
    assert(escapeCommandString(complex) === expected)
  }
  
  test("tokensToRun") {
    import CommandLineStringJob.tokensToRun
    
    def shDashC(s: String): Seq[String] = Seq("sh", "-c", s)
    
    assert(tokensToRun("") === shDashC(""))
    assert(tokensToRun("abc") === shDashC("abc"))
    assert(tokensToRun(" foo ") === shDashC(" foo "))
    
    assert(tokensToRun("$") === shDashC("""\\\\$"""))
    
    assert(tokensToRun("""\""") === shDashC("""\\\\"""))

    //scalastyle:off line.length
    val complex = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if($8 >= 0.0884) print $0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
    
    val expected = """(head -1 ./BIOME_AFFY.kinship.pruned.king.kin0 ; sed '1d' ./BIOME_AFFY.kinship.pruned.king.kin0 | awk '{if(\\\\$8 >= 0.0884) print \\\\$0}' | sort -rn -k8,8) > ./BIOME_AFFY.kinship.pruned.king.kin0.related"""
    //scalastyle:off line.length
    
    assert(tokensToRun(complex) === shDashC(expected))
  }
}