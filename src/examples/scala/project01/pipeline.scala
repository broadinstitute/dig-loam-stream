import values.greeting
import values.answer

object pipeline extends loamstream.LoamFile {
  cmd"echo $greeting! The answer is $answer."
}