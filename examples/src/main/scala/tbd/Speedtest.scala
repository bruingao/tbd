package tbd.examples

import scalaz._
import Scalaz._

object Speedtest {
  def wordcount1(lines: String) = {
    val counts = scala.collection.mutable.Map[String, Int]()

    for (word <- lines.split("\\W+")) {
      if (counts.contains(word)) {
        counts(word) += 1
      } else {
        counts(word) = 1
      }
    }

    counts
  }

  def wordcount2(lines: String) = {
    val split: Array[String] = lines.split("\\W+")

    def innerWordcount(i: Int, counts: Map[String, Int]): Map[String, Int] = {
      if (i < split.size) {
        val newCount = (split(i) -> (1 + counts.getOrElse(split(i), 0)))
        innerWordcount(i + 1, counts + newCount)
      } else {
        counts
      }
    }

    innerWordcount(0, Map[String, Int]())
  }

  def wordcount3(lines: String) = {
    val counts = new collection.mutable.HashMap[String, Int].withDefaultValue(0)
    lines.split("\\W+").foreach(word => counts(word) += 1)
    //scala.collection.immutable.HashMap(counts.toSeq: _*)
  }

  def reduce1(
      map1: scala.collection.mutable.Map[String, Int],
      map2: scala.collection.mutable.Map[String, Int]) = {
    val counts = map2.clone()
    for ((key, value) <- map1) {
      if (counts.contains(key)) {
        counts(key) += map1(key)
      } else {
        counts(key) = map1(key)
      }
    }
    counts
  }

  def reduce2(map1: Map[String, Int], map2: Map[String, Int]) = {
    (map1.keySet ++ map2.keySet).map (i=> (i,map1.getOrElse(i,0) + map2.getOrElse(i,0))).toMap
  }

  def reduce3(map1: scala.collection.immutable.HashMap[String, Int],
              map2: scala.collection.immutable.HashMap[String, Int]) = {
    map1.merged(map2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
  }

  def reduce4(map1: Map[String, Int], map2: Map[String, Int]) = {
    map1 |+| map2
  }

  def run[T](alg: () => T, desc: String): T = {
    val map = alg()
    val before = System.currentTimeMillis()
    for (i <- 1 to 100) {
      alg()
    }
    println(desc + "\t" + (System.currentTimeMillis() - before))
    map
  }

  def main(args: Array[String]) {
    val source = scala.io.Source.fromFile("input.txt")
    val lines = source.mkString
    source.close()

    val mutableMap1 = run(() => wordcount1(lines), "wordcount1")
    val map1 = Map[String, Int]() ++ mutableMap1

    val map2 = run(() => wordcount2(lines), "wordcount2")
    val mutableMap2 = collection.mutable.Map(map2.toSeq: _*)

    run(() => wordcount3(lines), "wordcount3")

    run(() => reduce1(mutableMap1, mutableMap2), "reduce1")

    run(() => reduce2(map1, map2), "reduce2")

    val hashmap1: scala.collection.immutable.HashMap[String, Int] =
        scala.collection.immutable.HashMap[String, Int]() ++ map1
    val hashmap2: scala.collection.immutable.HashMap[String, Int] =
        scala.collection.immutable.HashMap[String, Int]() ++ map2

    run(() => reduce3(hashmap1, hashmap2), "reduce3")

    run(() => reduce4(map1, map2), "reduce4")
  }
}
