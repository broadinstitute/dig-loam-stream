package loamstream.io

import loamstream.io.LIO.Decoder.MappedDecoder
import loamstream.io.LIO.Encoder.MappedEncoder
import loamstream.io.LIO.{Decoder, Encoder}
import loamstream.util.Shot

/**
  * LoamStream
  * Created by oliverr on 4/13/2016.
  */
object LIO {

  object Encoder {

    case class MappedEncoder[Conn, Ref, Maker, -T, TDel](delegateEncoder: Encoder[Conn, Ref, Maker, TDel],
                                                        fun: T => TDel)
      extends Encoder[Conn, Ref, Maker, T] {
      override def encode(io: LIO[Conn, Ref, Maker], thing: T): Ref = delegateEncoder.encode(io, fun(thing))
    }

  }

  trait Encoder[Conn, Ref, Maker, -T] {
    def encode(io: LIO[Conn, Ref, Maker], thing: T): Ref

    def map[TWrap, TMapped <: T](fun: TWrap => TMapped): MappedEncoder[Conn, Ref, Maker, TWrap, TMapped] =
      MappedEncoder(this, fun)
  }

  object Decoder {

    case class MappedDecoder[Conn, Ref, Maker, +T, TDel](delegateDecoder: Decoder[Conn, Ref, Maker, TDel],
                                                        fun: TDel => T)
      extends Decoder[Conn, Ref, Maker, T] {
      override def decode(io: LIO[Conn, Ref, Maker], ref: Ref): Shot[T] = delegateDecoder.decode(io, ref).map(fun(_))
    }

  }

  trait Decoder[Conn, Ref, Maker, +T] {
    def decode(io: LIO[Conn, Ref, Maker], ref: Ref): Shot[T]

    def map[TIn >: T, TWrap](fun: TIn => TWrap): MappedDecoder[Conn, Ref, Maker, TWrap, TIn] = MappedDecoder(this, fun)
  }

}

trait LIO[Conn, Ref, Maker] {
  def conn: Conn

  def maker: Maker

  def write[T](thing: T)(implicit encoder: Encoder[Conn, Ref, Maker, T]): Ref = encoder.encode(this, thing)

  def read[T](ref: Ref)(implicit decoder: Decoder[Conn, Ref, Maker, T]): Shot[T] = decoder.decode(this, ref)

}
