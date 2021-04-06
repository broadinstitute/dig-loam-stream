package loamstream.loam.intake

/**
 * @author clint
 * 26 Mar, 2021
 */
trait Serializer[A, B] extends (A => B)