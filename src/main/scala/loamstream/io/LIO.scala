package loamstream.io

import loamstream.io.LIO.{Decoder, Encoder}
import loamstream.util.shot.Shot

/**
  * LoamStream
  * Created by oliverr on 4/13/2016.
  */
object LIO {

  trait Encoder[Conn, Ref, Maker, T] {
    def encode(io: LIO[Conn, Ref, Maker], thing: T): Ref
  }

  trait Decoder[Conn, Ref, Maker, T] {
    def read(io: LIO[Conn, Ref, Maker], ref: Ref): Shot[T]
  }

}

trait LIO[Conn, Ref, Maker] {
  def conn: Conn

  def maker: Maker

  def write[T](thing: T)(implicit encoder: Encoder[Conn, Ref, Maker, T]): Ref = encoder.encode(this, thing)

  def read[T](ref: Ref)(implicit decoder: Decoder[Conn, Ref, Maker, T]): Shot[T] = decoder.read(this, ref)

}
