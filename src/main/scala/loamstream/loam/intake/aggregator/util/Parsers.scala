package loamstream.loam.intake.aggregator.util

import scala.util.matching.Regex
import java.net.URI
import java.nio.file.Paths
import scala.annotation.tailrec

/**
 * @author clint
 * Oct 9, 2020
 * 
 * Ported from util/parsers.py
 */
object Parsers {
  val CHROMOSOME: Regex = "(?i)^(?:chr)?(\\d{1,2}|x|y|xy|mt?)".r // was re.IGNORECASE, note leading (?i)
  val BIOSAMPLE: Regex = "^(?:CL|UBERON|EFO):\\d+".r
  val ENS_ID: Regex = "(?i)^(ENS[GTEP]\\d+)(?:\\..+)?".r // was re.IGNORECASE, note leading (?i)
  
  private object Regexes {
    val delimiters = "(?:[_\\-\\s]+|^)"
    
    val nameAndExt = "$(.*)\\.([^\\.]+)$".r
  }
  
  val BASE_PAIRS: Map[Char, Char] = Map(
    'A' -> 'T',
    'T' -> 'A',
    'G' -> 'C',
    'C' -> 'G',
  )
  
  /**
   * Parse a chromosome string and return it or raise a ValueError exception.
   */
  def parseChromosome(s: String): String = s match {
    case CHROMOSOME(chromosome) => chromosome.toUpperCase
    case _ => sys.error("Failed to match chromosome against '${s}'")
  }
  
  /**
   * Translate a string like "foo_Bar-baz  whee" and return "FooBarBazWhee".
   */
  def capCaseStr(s: String): String = {
    s.split(Regexes.delimiters).iterator.map(_.toLowerCase.capitalize).mkString("")
  }
  
  /**
   * Translate a string like "Foo_Bar-baz  whee" and return "fooBarBazWhee".
   */
  def camelCaseStr(s: String): String = {
    val capCased = capCaseStr(s)

    //force the first character to be lowercase
    s"${capCased.take(1).map(_.toLower)}${capCased.drop(1)}"
  }
  
  /**
   * Returns the file extension of a URL string.
   */
  def urlExt(s: String): String = {
    val asUri = URI.create(s)
    
    val pathOpt = Option(asUri.getPath).map(Paths.get(_))
    
    def getExt(fileName: String) = fileName match {
      case Regexes.nameAndExt(_, ext) => ext.toLowerCase
      case _ => "" //TODO ???
    }
    
    pathOpt.flatMap(p => Option(p.getFileName)).map(_.toString).map(getExt).getOrElse("")//TODO ???
    
    //return os.path.splitext(os.path.basename(urllib.parse.urlparse(s).path))[1].lower()
  }
  
  /**
   * Returns the root basename (e.g. '/foo/bar.baz.gz' -> 'bar').
   */
  @tailrec
  def basenameRoot(path: String): String = {
    path match {
      case Regexes.nameAndExt(name, ext) => if(ext.trim.isEmpty) name else basenameRoot(name)
      case _ => ??? //TODO 
    }
    //base, ext = os.path.splitext(os.path.basename(path))
    //# recurse if an extension was removed
    //return base if ext == '' else basename_root(base)
  }
  
  /**
   * Convert - strand alleles to + strand.
   */
  def complementAllele(allele: String): String = {
    allele.reverse.iterator.map(c => BASE_PAIRS(c.toUpper)).mkString("")
  
    //return str.join('', [BASE_PAIRS[c.upper()] for c in allele[::-1]])
  }
}
/*
 * import os.path
import re
import urllib.parse

CHROMOSOME = re.compile(r'^(?:chr)?(\d{1,2}|x|y|xy|mt?)', re.IGNORECASE)
BIOSAMPLE = re.compile(r'^(?:CL|UBERON|EFO):\d+')
ENS_ID = re.compile(r'^(ENS[GTEP]\d+)(?:\..+)?', re.IGNORECASE)
BASE_PAIRS = {
    'A': 'T',
    'T': 'A',
    'G': 'C',
    'C': 'G',
}


def parse_chromosome(s):
    """
    Parse a chromosome string and return it or raise a ValueError exception.
    """
    chromosome = CHROMOSOME.search(s)

    if not chromosome:
        raise ValueError('Failed to match chromosome against %s' % s)

    return chromosome.group(1).upper()


def cap_case_str(s):
    """
    Translate a string like "foo_Bar-baz  whee" and return "FooBarBazWhee".
    """
    return re.sub(r'(?:[_\-\s]+|^)(.)', lambda m: m.group(1).upper(), s)


def camel_case_str(s):
    """
    Translate a string like "Foo_Bar-baz  whee" and return "fooBarBazWhee".
    """
    s = cap_case_str(s)

    # force the first character to be lowercase
    return s[:1].lower() + s[1:]


def url_ext(s):
    """
    Returns the file extension of a URL string.
    """
    return os.path.splitext(os.path.basename(urllib.parse.urlparse(s).path))[1].lower()


def basename_root(path):
    """
    Returns the root basename (e.g. '/foo/bar.baz.gz' -> 'bar').
    """
    base, ext = os.path.splitext(os.path.basename(path))

    # recurse if an extension was removed
    return base if ext == '' else basename_root(base)


def complement_allele(allele):
    """
    Convert - strand alleles to + strand.
    """
    return str.join('', [BASE_PAIRS[c.upper()] for c in allele[::-1]])
 * 
 */
