package reductions

import scala.annotation._
import org.scalameter._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> false
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")
  }
}

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def withStack(chars: Array[Char], stack: List[Char]): Boolean =
      if chars.isEmpty then stack.isEmpty
      else if chars.head == ')' then
        if stack.isEmpty then false
        else withStack(chars.tail, stack.tail)
      else if chars.head == '(' then 
        withStack(chars.tail, stack :+ chars.head)
      else withStack(chars.tail, stack)
    
    val stack = List()
    withStack(chars, stack)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {
    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): (Int, Int) =
      var min = 0
      var acc = 0
      for (current <- idx until until) {
        if (chars(current) == '(') then acc += 1
        if (chars(current) == ')') then acc -= 1
        if (acc < min) then min = acc
      }
      (min, acc)

    def reduce(from: Int, until: Int): (Int, Int) = 
      if (until - from <= threshold) then traverse(from, until, 0, 0)
      else
        val middle = from + (until - from) / 2
        val (left, right) = parallel(reduce(from, middle), reduce(middle, until))
        (Math.min(left._1, left._2 + right._1), left._2 + right._2)

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
