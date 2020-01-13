package com.risk.arisk.stackoverflow

import com.risk.arisk.SparkInitialContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/** A raw stackoverflow posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, acceptedAnswer: Option[Int], parentId: Option[QID], score: Int, tags: Option[String]) extends Serializable


/** The main class */
object StackOverflow extends StackOverflow with SparkInitialContext {

 /* @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
  @transient lazy val sc: SparkContext = new SparkContext(conf)*/

  /** Main function */
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = sparkSession.sparkContext

    val lines   = sc.textFile("src/main/resources/stackoverflow/stackoverflow.csv")
    val raw     = rawPostings(lines)
    raw.cache()

    val grouped = groupedPostings(raw)
    grouped.cache()

    val scored  = scoredPostings(grouped)
    scored.cache()


    val vectors = vectorPostings(scored)
    assert(vectors.count() == 2121822, "Incorrect number of vectors: " + vectors.count())

    vectors.take(10).foreach(println)

    val means   = kmeans(sampleVectors(vectors), vectors, debug = true)
    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class StackOverflow extends StackOverflowInterface with Serializable {

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


  /** Group the questions and answers together */
  def groupedPostings(postings: RDD[Posting]): RDD[(QID, Iterable[(Question, Answer)])] = {

    val questionRdd:RDD[(Int,Posting)] = postings.filter(p => p.postingType == 1 ).map( p =>  (p.id , p))
    val answerRdd:RDD[(Int,Posting)] = postings.filter(p => p.postingType == 2  && !p.parentId.isEmpty ).map( p =>  (p.parentId.get, p))


    //question rdd
    //questionRdd.collect().foreach(println)

    //answer rdd
    //println("answers getting printed now ")
    //answerRdd.collect().foreach(println)

    val questionAndAnswer = questionRdd.join(answerRdd)

    //println("question and answers joined getting printed now ")
    //questionAndAnswer.collect().foreach(println)

    val qAndAIterable = questionAndAnswer.groupByKey()

   // qAndAIterable.collect.foreach(println)

    return qAndAIterable
  }


  /** Compute the maximum score for each posting */
  def scoredPostings(grouped: RDD[(QID, Iterable[(Question, Answer)])]): RDD[(Question, HighScore)] = {

    def answerHighScore(as: Array[Answer]): HighScore = {
      //println("printing inside answerHighScore ")

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

    val scoredItems = grouped.map {
      case (key, iter) => {
        val iterator: Iterator[(Question, Answer)] =  iter.iterator
        var question:Question = null;
        var array:ListBuffer[Answer] = new ListBuffer[Answer]()
        while(iterator.hasNext){
          val nextEle = iterator.next()
          if(question == null ) {
              question = nextEle._1
            }
          array+= nextEle._2
        }

       // println(s" question Id ${question.id} , array length ${array.toList.toArray.length}")
        (question, answerHighScore(array.toList.toArray))
      }
    }

    println(" Just before printing the new value for scored RDD "+ scoredItems.count())
    //scoredItems.collect.foreach(println)

    if(!scoredItems.take(1).isEmpty){
      scoredItems
    }else {
      null
    }
  }


  /** Compute the vectors for the kmeans */
  def vectorPostings(scored: RDD[(Question, HighScore)]): RDD[(LangIndex, HighScore)] = {

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

    if(!scored.take(1).isEmpty){
      scored.map{
        case (x) => {
             val tag = x._1.tags
            //Revisit this later
             val langIndex = (firstLangInTag(tag,langs).getOrElse(0) * langSpread)
            (langIndex,x._2)
        }
      }
    }else {
      null
    }
  }


  /** Sample the vectors */
  def sampleVectors(vectors: RDD[(LangIndex, HighScore)]): Array[(Int, Int)] = {

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
    val newMeans = means.clone() // you need to compute newMeans

    // TODO: Fill in the newMeans array
    val oldClusters = vectors.map(x=>(findClosest(x,means),x)).groupByKey()
    val newClusters = oldClusters.map(x=>(x._1,averageVectors(x._2))).collect()

    for(eachCluster<-newClusters) {
      newMeans.update(eachCluster._1, eachCluster._2)
    }

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
      if (debug) {
        println("Reached max iterations!")
      }
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

  /** Return the euclidean distance between two points */
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




  //
  //
  //  Displaying results:
  //
  //
  def clusterResults(means: Array[(Int, Int)], vectors: RDD[(LangIndex, HighScore)]): Array[(String, Double, Int, Int)] = {
    val closest = vectors.map(p => (findClosest(p, means), p))
    val closestGrouped = closest.groupByKey()


    def mostCommonLanguage(vectorVal: Iterable[(Int,Int)]): (Int,Int) = {

      def mostCommonlangInner(innerVectorVal: Iterable[(Int, Int)],langCount: Array[Int]): (Int, Int) = {
          for(elem<-innerVectorVal) {
            val index = elem._1/langSpread
            langCount(index)=langCount(index)+ 1
          }
          (langCount.indexOf(langCount.max), langCount.max)
        }
      mostCommonlangInner(vectorVal,new Array[Int](langs.length))

    }


    def calculateMedian(vectors :Iterable[(Int, Int)]): Int = {
      val allScoresArr = vectors.map(x=>x._2).toArray
      val sortedArr = allScoresArr.sortWith(_<_)
      val arrLength = sortedArr.length
      if(arrLength%2==0)
        (sortedArr(arrLength/2-1)+sortedArr(arrLength/2))/2
      else sortedArr((arrLength-1)/2)
    }



    val median = closestGrouped.mapValues { vs =>

      val mostCommonlangIndex = mostCommonLanguage(vs)
      val langLabel: String   = langs(mostCommonlangIndex._1) // most common language in the cluster
      val clusterSize: Int    = vs.size
      val langPercent: Double = mostCommonlangIndex._2*100d/ clusterSize
      val medianScore: Int    = calculateMedian(vs)

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
