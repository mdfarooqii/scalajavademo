package com.risk.arisk

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer


case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.toLowerCase.split(' ').contains(lang)
}

object WikipediaRanking extends SparkInitialContext with WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")


  val sc: SparkContext = sparkSession.sparkContext

  // Hint: use a combination of `sc.parallelize`, `WikipediaData.lines` and `WikipediaData.parse`
  val wikiRdd: RDD[WikipediaArticle] = sc.parallelize(WikipediaData.lines.map(line => WikipediaData.parse(line) ))
  wikiRdd.cache()


  /** Returns the number of articles on which the language `lang` occurs.
   *  Hint1: consider using method `aggregate` on RDD[T].
   *  Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
   */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    var langCount:Long = 0;
    if(rdd.take(1) != null){
      //aggregate
      def param3= (accu:Int, v:(String,Int)) => accu + v._2
      def param4= (accu1:Int,accu2:Int) => accu1 + accu2
      langCount = rdd.filter(a => a.mentionsLanguage(lang.toLowerCase())).map(a => (lang,1)).aggregate(0)(param3,param4)
    }
    langCount.toInt
  }



  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    var descLangList: ListBuffer[(String,Int)] = new ListBuffer[(String,Int)]()
    for (lang <- langs) yield  {
      val count = occurrencesOfLang(lang,wikiRdd)
      descLangList += (Tuple2(lang,count))
    }
    descLangList.toList.sortBy(_._2)(Ordering[Int].reverse)
  }


 /* Compute an inverted index of the set of articles, mapping each language
  * to the Wikipedia pages in which it occurs.
  */
 def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
   var resultRdd : RDD[(String, Iterable[WikipediaArticle])] = sc.emptyRDD

   for(lang1 <- langs) {
     val langRdd = rdd.filter(a =>  a.mentionsLanguage(lang1.toLowerCase())).groupBy((x : WikipediaArticle) => (lang1))
     //val langRdd1 = rdd.filter(a =>  a.mentionsLanguage(lang1.toLowerCase())).map(x =>  (lang1, (x,lang1 ))).reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
     resultRdd = resultRdd.union(langRdd)
   }
   resultRdd.cache()
   resultRdd
 }


 /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
  *     a performance improvement?
  *
  *   Note: this operation is long-running. It can potentially run for
  *   several seconds.
  */


 def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
   val changedIndex = index.map{case (x, iter) => (x, iter.toList.length)}
   changedIndex.collect.toList
 }

 /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
  *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
  *     and the computation of the ranking? If so, can you think of a reason?
  *
  *   Note: this operation is long-running. It can potentially run for
  *   several seconds.
  */


 def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
   val myList: ListBuffer[(String,Int)] = new ListBuffer[(String, Int)]()
   for(lang <- langs) {
     val reducedRdd = rdd.filter(a => a.mentionsLanguage(lang.toLowerCase())).map(a => (lang, 1)).reduceByKey(_+_).collect()
     if(reducedRdd !=null && !reducedRdd.isEmpty){
       myList += Tuple2(reducedRdd(0)._1, reducedRdd(0)._2)
     }
   }
   myList.toList.sortBy(_._2)(Ordering[Int].reverse)
 }

  def main(args: Array[String]): Unit = {
    println("I am inside the main method")

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
