/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbd.mod

import scala.collection.mutable.{ArrayBuffer, Buffer}

import tbd.{Changeable, Changeable2, Context, Memoizer}
import tbd.Constants.ModId
import tbd.TBD._

class ModList[T, U](
    val head: Mod[ModListNode[T, U]]
  ) extends AdjustableList[T, U] {

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): ModList[T, U] = {
    val memo = makeMemoizer[Mod[ModListNode[T, U]]]()

    new ModList(
      mod {
        read(head) {
	  case null => write[ModListNode[T, U]](null)
	  case node => node.filter(pred, memo)
        }
      }
    )
  }

  def map[V, W](
      f: ((T, U)) => (V, W))
     (implicit c: Context): ModList[V, W] = {
    val memo = makeMemoizer[Changeable[ModListNode[V, W]]]()

    new ModList(
      mod {
        read(head) {
          case null => write[ModListNode[V, W]](null)
          case node => node.map(f, memo)
        }
      }
    )
  }

  def reduce(
      identityMod: Mod[(T, U)],
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)] = {

    // Each round we need a hasher and a memo, and we need to guarantee that the
    // same hasher and memo are used for a given round during change propagation,
    // even if the first mod of the list is deleted.
    class RoundMemoizer {
      val memo = makeMemoizer[(Hasher,
                               Memoizer[Mod[ModListNode[T, U]]],
                               RoundMemoizer)]()

      def getTuple() =
        memo() {
          (new Hasher(2, 4),
           makeMemoizer[Mod[ModListNode[T, U]]](),
           new RoundMemoizer())
	}
    }

    def randomReduceList(
        head: ModListNode[T, U],
        identity: (T, U),
        round: Int,
        roundMemoizer: RoundMemoizer): Changeable[(T, U)] = {
      val tuple = roundMemoizer.getTuple()

      val halfListMod =
        mod {
          read(identityMod) {
	    case identity => halfList(identity, identity, head, round, tuple._1, tuple._2)
	  }
	}

      read(halfListMod)(halfList =>
        read(halfList.next)(next =>
          if(next == null)
            write(halfList.value)
          else
            randomReduceList(halfList, identity, round + 1, tuple._3)))
    }

    def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
      hasher.hash(id.hashCode() ^ round) == 0
    }

    def halfList(
        acc: (T, U),
        identity: (T, U),
        head: ModListNode[T, U],
        round: Int,
        hasher: Hasher,
        memo: Memoizer[Mod[ModListNode[T, U]]]
      ): Changeable[ModListNode[T, U]] = {
      val newAcc = f(acc, head.value)

      if(binaryHash(head.next.id, round, hasher)) {
        val newNext = memo(head.next, identityMod) {
	  mod {
	    read(head.next)(next =>
	      if (next == null)
	        write[ModListNode[T, U]](null)
	      else
	          halfList(identity, identity, next, round,
                           hasher, memo))
	  }
	}
        write(new ModListNode(newAcc, newNext))
      } else {
        read(head.next)(next =>
	  if (next == null) {
	    val newNext = createMod[ModListNode[T, U]](null)
            write(new ModListNode(newAcc, newNext))
	  } else {
	    halfList(newAcc, identity, next, round, hasher, memo)
	  }
        )
      }
    }

    val roundMemoizer = new RoundMemoizer()
    mod {
      read(identityMod)(identity =>
        read(head)(head =>
          if(head == null)
            read(identityMod)(identity =>
              write(identity))
          else
            randomReduceList(head, identity, 0, roundMemoizer)))
    }
  }

  def sort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): AdjustableList[T, U] = {
    val memo = makeMemoizer[Mod[ModListNode[T, U]]]()

    val sorted = mod {
      read(head) {
        case null => write[ModListNode[T, U]](null)
        case node =>
	  node.sort(createMod(null), comparator, memo)
      }
    }

    new ModList(sorted)
  }

  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U]) = {
    val memo = makeMemoizer[Changeable2[ModListNode[T, U], ModListNode[T, U]]]()

    val result = mod2(2) {
      read(head) {
	case null =>
	  write2(null.asInstanceOf[ModListNode[T, U]],
		 null.asInstanceOf[ModListNode[T, U]])
	case node => 
	  memo(node) {
	    node.split(memo, pred)
	  }
      }
    }

    (new ModList(result._1), new ModList(result._2))
  }

  def toBuffer(): Buffer[U] = {
    val buf = ArrayBuffer[U]()
    var node = head.read()
    while (node != null) {
      buf += node.value._2
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
