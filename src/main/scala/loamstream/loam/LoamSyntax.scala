package loamstream.loam

import loamstream.compiler.LoamPredef
import loamstream.util.Paths
import loamstream.util.Uris
import loamstream.googlecloud.GoogleHelpers
import loamstream.googlecloud.HailLoamSyntax
import software.amazon.awssdk.awscore.util.AwsHeader
import loamstream.loam.aws.AwsHelpers

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
    HailLoamSyntax with 
    AwsHelpers
    
object LoamSyntax extends LoamSyntax
