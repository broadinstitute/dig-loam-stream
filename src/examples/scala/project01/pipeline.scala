import values.greeting
import values.answer

object pipeline extends loamstream.loam.asscala.LoamFile {
  cmd"echo $greeting! The answer is $answer."
}