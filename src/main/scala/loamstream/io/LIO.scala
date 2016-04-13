package loamstream.io

import loamstream.io.LIO.{Decoder, Encoder}
import loamstream.util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 4/13/2016.
  */
object LIO {

  trait Encoder[Ref, T] {
    def encode(io: LIO[Ref], thing: T): Ref
  }

  trait Decoder[Ref, T] {
    def read(io: LIO[Ref], ref: Ref): Shot[T]
  }

}

trait LIO[Ref] {

  def write[T](thing: T)(implicit encoder: Encoder[Ref, T]): Ref = encoder.encode(this, thing)

  def read[T](ref: Ref)(implicit decoder: Decoder[Ref, T]): Shot[T] = decoder.read(this, ref)

  def writeDouble(double: Double): Ref

  def readDouble(ref: Ref): Shot[Double]

}
