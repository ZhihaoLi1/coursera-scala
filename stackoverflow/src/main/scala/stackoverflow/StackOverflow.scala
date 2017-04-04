package stackoverflow

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
//    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow-sample2.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
//    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)

    sc.stop()
  }

}


/** The parsing and kmeans methods */
class StackOverflow extends Serializable {

  /** Languages */
  val langs =
    List(
      "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
      "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def langSpread = 50000
  assert(langSpread > 0, "If langSpread is zero we can't recover the language from the input data!")

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria */
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120


  //
  //
  // Parsing utilities:
  //
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
              id =             arr(1).toInt,
              acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
              parentId =       if (arr(3) == "") None else Some(arr(3).toInt),
              score =          arr(4).toInt,
              tags =           if (arr.length >= 6) Some(arr(5).intern()) else None)
    })


  /** Group the questions and answers together
    * 1. filter questions and answers separately
    * 2. extract QID value in the first element of a tuple
    * 3. join to obtain RDD[(QID, (Question, Answer))]
    * 4. groupByKey, reduceByKey etc. to obtain RDD[(QID, Iterable[(Question, Answer)])] */
  def groupedPostings(postings: RDD[Posting]): RDD[(Int, Iterable[(Posting, Posting)])] = {
    val postings2 = postings.cache()
    val questionRdd = postings2.filter(post => post.postingType == 1).map(post => (post.id, post))
    val answerRdd   = postings2.filter(post => post.postingType == 2) // && post.parentId != None)
      .map(post => (post.parentId.get, post))
    /** 03_optimizing-with-partitioners_spark-3-3.pdf
      * todo: manually set partition tie breaks based on binned post ids
      */
    val tunedPartitioner = new RangePartitioner(8, questionRdd)
    val questionRddPart = questionRdd.partitionBy(tunedPartitioner).persist()
    val answerRddPart = answerRdd.partitionBy(tunedPartitioner).persist()
//    val joined =
    questionRddPart.join(answerRddPart).partitionBy(tunedPartitioner).persist().groupByKey()
//    val grouped     = joined.groupByKey
//    grouped
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(Int, Iterable[(Posting, Posting)])]): RDD[(Posting, Int)] = {

    def answerHighScore(as: Array[Posting]): Int = {
      var highScore = 0
          var i = 0
          while (i < as.length) {
            val score = as(i).score
                if (score > highScore)
                  highScore = score
                  i += 1
          }
      highScore
    }
    /** using mapValue and join */
//    val groupedQuestions = grouped.mapValues(xs => xs.map( { case (q, a) => q }).toArray.head
//    val groupedAnswers = grouped.mapValues(xs => xs.map( { case (q, a) => a }))
//    val scoredAnswers = groupedAnswers.mapValues(xs => answerHighScore(xs.toArray))
//    val scored = groupedQuestions.join(scoredAnswers).map(_._2)
    /** Learning Spark p.50 rdd.values() Return an RDD of just the values */
//    val split =
      grouped.values.map(qa => (qa.head._1, qa.map(_._2)))
//    val scored =
        .map( { case (q, answers) => (q, answerHighScore(answers.toArray)) })
//    scored
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Posting, Int)]): RDD[(Int, Int)] = {
    /** Return optional index of first language that occurs in `tags`. */
    def firstLangInTag(tag: Option[String], ls: List[String]): Option[Int] = {
      if (tag.isEmpty) None
      else if (ls.isEmpty) None
      else if (tag.get == ls.head) Some(0) // index: 0
      else {
        val tmp = firstLangInTag(tag, ls.tail)
        tmp match {
          case None => None
          case Some(i) => Some(i + 1) // index i in ls.tail => index i+1
        }
      }
    }
    val scored2 = scored.cache()
//    val vectors =
      scored2
      .map({ case (post, rating) => (firstLangInTag(post.tags, langs), rating) })
      .filter(xs => xs._1.nonEmpty)
      .map({ case (tag, rating) => (tag.get * langSpread, rating) })
//    vectors
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(Int, Int)]): Array[(Int, Int)] = {

    assert(kmeansKernels % langs.length == 0, "kmeansKernels should be a multiple of the number of languages studied.")
    val perLang = kmeansKernels / langs.length

    // http://en.wikipedia.org/wiki/Reservoir_sampling
    def reservoirSampling(lang: Int, iter: Iterator[Int], size: Int): Array[Int] = {
      val res = new Array[Int](size)
      val rnd = new util.Random(lang)

      for (i <- 0 until size) {
        assert(iter.hasNext, s"iterator must have at least $size elements")
        res(i) = iter.next
      }

      var i = size.toLong
      while (iter.hasNext) {
        val elt = iter.next
        val j = math.abs(rnd.nextLong) % i
        if (j < size)
          res(j.toInt) = elt
        i += 1
      }

      res
    }

    val res =
      if (langSpread < 500)
        // sample the space regardless of the language
        vectors.takeSample(false, kmeansKernels, 42)
      else
        // sample the space uniformly from each language partition
        vectors.groupByKey.flatMap({
          case (lang, vectors) => reservoirSampling(lang, vectors.toIterator, perLang).map((lang, _))
        }).collect()

    assert(res.length == kmeansKernels, res.length)
    res
  }


  //
  //
  //  Kmeans method:
  //
  //

  /** Main kmeans computation */
  @tailrec final def kmeans(means: Array[(Int, Int)], vectors: RDD[(Int, Int)], iter: Int = 1, debug: Boolean = false): Array[(Int, Int)] = {
    // TODO: Fill in the newMeans array
    val newMeans = means.clone() // you need to compute newMeans
    /** pairing each vector with the index of the closest mean (its cluster)
      * Return the closest point
      * findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int
      * */
    val vectors2 = vectors.cache()
//    val vectorMeans = vectors2.map(xs => (findClosest(xs, newMeans), xs)) // PairRDD[(Int, (Int, Int))]
//
////    val tunedPartitioner = new RangePartitioner(8, vectorMeans)
////    val vectorMeansPart = vectorMeans.partitionBy(tunedPartitioner).persist()
//
//    /** computing the new means by averaging the vectors that form each cluster
//      * Average the vectors
//      * averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) */
//    val averaged = vectorMeans.groupByKey.mapValues(averageVectors(_)).collect()
//    // val grouped = vectorMeansPart.groupByKey.values
//    averaged.foreach({ case (idx, p) => newMeans.update(idx, p) })
//
    /** computing the new means by averaging the vectors that form each cluster
      * Average the vectors
      * averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) */
    vectors2.map(xs => (findClosest(xs, newMeans), xs)) // PairRDD[(Int, (Int, Int))]
    .groupByKey.mapValues(averageVectors(_)).collect()
    .foreach({ case (idx, p) => newMeans.update(idx, p) })


    /** euclideanDistance: Return the euclidean distance between two arrays */
    val distance = euclideanDistance(means, newMeans)

    if (debug) {
      println(s"""Iteration: $iter
                 |  * current distance: $distance
                 |  * desired distance: $kmeansEta
                 |  * means:""".stripMargin)
      for (idx <- 0 until kmeansKernels)
      println(f"   ${means(idx).toString}%20s ==> ${newMeans(idx).toString}%20s  " +
              f"  distance: ${euclideanDistance(means(idx), newMeans(idx))}%8.0f")
    }

    if (converged(distance))
      newMeans
    else if (iter < kmeansMaxIterations)
      kmeans(newMeans, vectors, iter + 1, debug)
    else {
      println("Reached max iterations!")
      newMeans
    }
  }




  //
  //
  //  Kmeans utilities:
  //
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) =
    distance < kmeansEta


  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the euclidean distance between two arrays */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- 0 until centers.length) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }


  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }


  /** Calculate median for vector */
//  def medianVectors(ps: Iterable[(Int, Int)]): Int = {
//    // val sortedScore = rdd.map(_._2).sortBy(r => r).collect().toSeq // sortBy(r => r._2)
//    val sortedScore = ps.map(_._2).toSeq .sortWith(_ < _)
//    val halfLen = sortedScore.length / 2
//    val even = (sortedScore.length % 2 == 0)
//    val message = List("even", "uneven")
//    val median =
//      if (even) {
//        // println(message(0))
//        (sortedScore(halfLen - 1) + sortedScore(halfLen)) / 2 // return Double: 2.0
//      } else {
//        // println(message(1))
//        sortedScore(halfLen) // index starts at 0
//      }
//    median
//  }
  def medianVectors(s: Seq[Double]) = {
    val (lower, upper) =
       s.sortWith(_ < _).
       splitAt(s.size / 2)
       if (s.size % 2 == 0)
         (lower.last + upper.head) / 2.0
       else upper.head
   }




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(Int, Int)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p)) // RDD[Int, (Int, Int)]
    val closestGrouped = closest.groupByKey()

    val median = closestGrouped.mapValues { vs =>
      // most common language in the cluster
      val langIndex: Int      =
        vs.groupBy(xs => xs._1)
          .map(xs => (xs._1, xs._2.size))
          .maxBy(xs => xs._1)._1 / langSpread
      val langLabel: String   = langs(langIndex)
      val clusterSize: Int    = vs.size

      // percent of the questions in the most common language
      val langPercent: Double =
        vs.map( { case (v1, v2) => v1 })
          .filter(v1 => v1 == langIndex * langSpread).size * 100 / clusterSize
      val medianScore: Int    = medianVectors(vs.map(_._2.toDouble).toSeq).toInt
//      val medianScore: Int    = 1

      (langLabel, langPercent, clusterSize, medianScore)
    }

    median.collect().map(_._2).sortBy(_._4)
  }

  def printResults(results: Array[(String, Double, Int, Int)]): Unit = {
    println("Resulting clusters:")
    println("  Score  Dominant language (%percent)  Questions")
    println("================================================")
    for ((lang, percent, size, score) <- results)
      println(f"${score}%7d  ${lang}%-17s (${percent}%-5.1f%%)      ${size}%7d")
  }
}
