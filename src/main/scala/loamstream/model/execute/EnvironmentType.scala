package loamstream.model.execute

import spire.syntax.truncatedDivision

/**
 * @author clint
 *         Nov 22, 2016
 */
sealed abstract class EnvironmentType private (val name: String) {
  final def isLocal: Boolean = this == EnvironmentType.Local

  final def isGoogle: Boolean = this == EnvironmentType.Google
  
  final def isUger: Boolean = this == EnvironmentType.Uger
  
  final def isLsf: Boolean = this == EnvironmentType.Lsf
  
  final def isSlurm: Boolean = this == EnvironmentType.Slurm

  final def isDrm: Boolean = isUger || isLsf || isSlurm 
  
  final def isAws: Boolean = this == EnvironmentType.Aws

  final def matches(resources: Option[Resources]): Boolean = resources match {
    case None => true
    case Some(rs) => matches(rs)
  }

  def matches(resources: Resources): Boolean
}

object EnvironmentType {
  object Names {
    val Local = "local"
    val Google = "google"
    val Uger = "uger"
    val Lsf = "lsf"
    val Slurm = "slurm"
    val Aws = "aws"
  }

  final case object Local extends EnvironmentType(Names.Local) {
    override def matches(resources: Resources): Boolean = resources.isInstanceOf[Resources.LocalResources]
  }

  final case object Uger extends EnvironmentType(Names.Uger) {
    override def matches(resources: Resources): Boolean = resources.isInstanceOf[Resources.UgerResources]
  }
  
  final case object Lsf extends EnvironmentType(Names.Lsf) {
    override def matches(resources: Resources): Boolean = resources.isInstanceOf[Resources.LsfResources]
  }
  
  final case object Slurm extends EnvironmentType(Names.Slurm) {
    override def matches(resources: Resources): Boolean = resources.isInstanceOf[Resources.SlurmResources]
  }

  final case object Google extends EnvironmentType(Names.Google) {
    override def matches(resources: Resources): Boolean = resources.isInstanceOf[Resources.GoogleResources]
  }
  
  final case object Aws extends EnvironmentType(Names.Aws) {
    override def matches(resources: Resources): Boolean = resources.isInstanceOf[Resources.AwsResources]
  }
  
  def fromString(s: String): Option[EnvironmentType] = s.trim.toLowerCase match {
    case Names.Local => Some(Local)
    case Names.Google => Some(Google)
    case Names.Uger => Some(Uger)
    case Names.Lsf => Some(Lsf)
    case Names.Slurm => Some(Slurm)
    case Names.Aws => Some(Aws)
    case _ => None
  }
}
