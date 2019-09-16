package loamstream.loam

import loamstream.compiler.LoamPredef
import loamstream.util.Paths
import loamstream.util.Uris
import loamstream.googlecloud.GoogleHelpers
import loamstream.googlecloud.HailLoamSyntax

/**
 * @author clint
 * Jul 31, 2019
 */
trait LoamSyntax extends 
    LoamTypes with
    LoamPredef with 
    LoamCmdSyntax with 
    Paths.Implicits with 
    Uris.Implicits with 
    GoogleHelpers with 
    HailLoamSyntax
    
object LoamSyntax extends LoamSyntax
