package akka

object Utils {

  def first8Chars(str: String): String = if (str.size > 8) str.substring(0, 8) else str

}
