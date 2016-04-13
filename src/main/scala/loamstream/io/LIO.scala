package loamstream.io

import loamstream.io.LIO.{Decoder, Encoder}
import loamstream.util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 4/13/2016.
  */
object LIO {

  trait Encoder[Ref, Maker, T] {
    def encode(io: LIO[Ref, Maker], thing: T): Ref
  }

  trait Decoder[Ref, Maker, T] {
    def read(io: LIO[Ref, Maker], ref: Ref): Shot[T]
  }

}

trait LIO[Ref, Maker] {
  def maker: Maker

  def write[T](thing: T)(implicit encoder: Encoder[Ref, Maker, T]): Ref = encoder.encode(this, thing)

  def read[T](ref: Ref)(implicit decoder: Decoder[Ref, Maker, T]): Shot[T] = decoder.read(this, ref)

}
