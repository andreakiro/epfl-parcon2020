package lockfree

import scala.annotation.tailrec

class SortedList extends AbstractSortedList {

  // The sentinel node at the head.
  private val _head: Node = createNode(0, None, isHead = true)

  // The first logical node is referenced by the head.
  def firstNode: Option[Node] = _head.next

  // Finds the first node whose value satisfies the predicate.
  // Returns the predecessor of the node and the node.
  def findNodeWithPrev(pred: Int => Boolean): (Node, Option[Node]) = 
    def iterate(prev: Node, cur: Option[Node]): (Node, Option[Node]) =
      (prev, cur) match
        case (before, Some(n)) =>
          if (n.deleted) then
            before.atomicState.compareAndSet((cur, false), (n.next, false))
            findNodeWithPrev(pred)
          else if pred(n.value) then (prev, cur) 
          else iterate(n, n.next)
        case (last, None) => (last, None)
    iterate(_head, firstNode)

  // Insert an element in the list.
  def insert(element: Int): Unit =
    val (prev, succ) = findNodeWithPrev(element <= _)
    val newNode: Node = createNode(element, succ)
    if (! prev.atomicState.compareAndSet((succ, false), (Some(newNode), false))) insert(element)

  // Checks if the list contains an element.
  def contains(element: Int): Boolean = 
    findNodeWithPrev(element == _)._2 match
      case Some(n) => ! n.deleted
      case None => false

  // Delete an element from the list.
  // Should only delete one element when multiple occurences are present.
  def delete(element: Int): Boolean =
    findNodeWithPrev(element == _)._2 match
      case Some(n) => if (n.mark) true else delete(element)
      case None => false
}
