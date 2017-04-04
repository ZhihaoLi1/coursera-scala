package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import scala.collection.immutable.HashMap

case class WikipediaArticle(title: String, text: String)

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("WikipediaRanking")
  val sc: SparkContext = new SparkContext(conf)
  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(WikipediaData.parse)

  /** Returns the number of articles on which the language `lang` occurs.
    * val result = encyclopedia.filter(page => page.contains("EPFL"))
    *                          .count()
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: should you count the "Java" language when you see "JavaScript"?
   *  Hint3: the only whitespaces are blanks " "
   *  Hint4: no need to search in the title :)
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    /**
      * Advanced Analytics with Spark, p.103
      * val zero = new HashMap[String, Int]()
      * def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int]): HashMap[String, Int] = {
      *   tfs.keySet.foreach {
      *     term => dfs += term -> (dfs.getOrElse(term, 0) + 1)
      *   }
      *   dfs
      * }
      * def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int]): HashMap[String, Int] = {
      *   for ((term, count) <- dfs2) {
      *     dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
      *   }
      *   dfs1
      * }
      * docTermFreqs.aggregate(zero)(merge, comb)
      *
      * Learning Spark, p.40
      * val input = sc.parallelize(List(1, 2, 3, 4))
      * val result = input.aggregate((0, 0))(
      *                    (acc, value) => (acc._1 + value, acc._2 + 1),
      *                    (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      * val avg = result._1 / result._2.toDouble
      */

    val result = rdd.aggregate(0)(
      (acc, page) => if(page.text.contains(lang)) acc + 1 else acc,
      (acc1, acc2) => acc1 + acc2
    )
    result
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operatn is long-running. It can potentially run for
   *   several seconds.io
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    val result =
      langs
        .map(lang => (lang, occurrencesOfLang(lang, rdd)))
        .sortWith(_._2 > _._2)
    result
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
  /**
    * Advanced Analytics with Spark, p.104
    * import org.apache.spark.mllib.linalg.Vectors
    * val vecs = docTermFreqs.map(termFreqs => {
    *   val docTotalTerms = termFreqs.values().sum
    *   val termScores = termFreqs.filter {
    *     case (term, freq) => bTermIds.containsKey(term)
    *   }.map{
    *     case (term, freq) => (bTermIds(term),
    *     bIdfs(term) * termFreqs(term) / docTotalTerms)
    *   }.toSeq
    *   Vectors.sparse(bTermIds.size, termScores)
    * })
    */
  /**
    * Scala Functional Programming Patterns, p.219
    * object InvertedIdx extends App {
    *   val m = List("Carr" -> "And So To Murder",
    *   "Carr" -> "The Arabian Nights Murder",
    *   "Carr" -> "The Mad Hatter Mystery",
    *   "Christie" -> "The Murder Of Roger Ackroyd",
    *   "Christie" -> "The Sittaford Mystery",
    *   "Carr" -> "The Plague Court Murders")
    * val ignoreWordsSet = Set("To", "The", "And", "So", "Of")
    * val invertedIdx = (
    *   for {
    *     (k, v) <- m
    *     w <- v.split("\\W")
    *     if !ignoreWordsSet.contains(w)
    *   }
    *   yield(w -> k)
    * ).groupBy(_._1).map {
    *   case (k,v) => (k,v.map(_._2))
    * }
    * println(invertedIdx)
    */
  /**
    * https://gist.github.com/Cowa/c3c7fc4d9cb60843ad37
    * lines.map(_.split(" "))
    *   .flatMap(x => x.drop(1).map(y => (y, x(0))))
    *   .groupBy(_._1)
    *   .map(p => (p._1, p._2.map(_._2).toVector))
    */
  /**
    * http://backtobazics.com/big-data/spark/apache-spark-groupby-example/
    * val y = x.groupBy(word => word.charAt(0))
    */
//    val result = for (lang <- langs) yield (lang, rdd.filter(article => article.text.contains(lang)).collect)
//    sc.parallelize(result).groupByKey
//    val str_it = for (lang <- langs) yield (
//      lang,
//      rdd.filter(article => article.text.contains(lang)).collect.toIterable
//    )
//    sc.parallelize(str_it)
    rdd.flatMap(article => langs.filter(lang => article.text.split(" ").contains(lang))
      .map(lang => (lang, article)))
      .groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    val result = for (idx <- index) yield (
      idx._1, idx._2.toArray.length
    )
    result.collect.toList.sortWith(_._2 > _._2)
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    /**
      * Advanced Analytics with Spark, p.228
      * val tokenized = sc.textFile(args(0)).flatMap(_.split(' '))
      * val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)
      * val filtered = wordCounts.filter(_._2 >= 1000)
      * val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).
      * reduceByKey(_ + _)
      * charCounts.collect()
      */
    /**
      * 02_pair-rdds.mp4 5:50
      * rdd.map(page => (page.title, page.text))
      */
    rdd.flatMap(page => langs
      .filter(lang => page.text.contains(lang))
      .map(lang => (lang, 1)))
      .reduceByKey(_ + _)
      .collect.toList.sortWith(_._2 > _._2)
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
