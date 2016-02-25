package loamstream.util

/**
  * LoamStream
  * Created by oliverr on 1/13/2016.
  */
object Index {

  trait Next[I <: Index] extends Index

  type I00 = Next[Nothing]
  type I01 = Next[I00]
  type I02 = Next[I01]
  type I03 = Next[I02]
  type I04 = Next[I03]
  type I05 = Next[I04]
  type I06 = Next[I05]
  type I07 = Next[I06]
  type I08 = Next[I07]
  type I09 = Next[I08]
  type I10 = Next[I09]
  type PlusTen[I <: Index] = Next[Next[Next[Next[Next[Next[Next[Next[Next[Next[I]]]]]]]]]]
}

sealed trait Index {

}
