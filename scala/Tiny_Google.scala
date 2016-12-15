import org.apache.spark.rdd._
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.fs.{FileSystem,Path}

var wordRDD : RDD[(String, String, Double)] = sc.parallelize(List((" "," ", 0.0)))

def getIndex(){
  var tempRDD : RDD[(String, Double)] = sc.parallelize(List((" ",0.0)))

  val keyword = readLine("What is the word you are looking for? ").toString
  println(keyword)
  val keywords = keyword.split("\\s+")
  println(keywords)
  for(key <- keywords){
    val queryRDD = wordRDD.filter(_._1.equals(key)).map(x => (x._2, x._3))
    tempRDD = tempRDD.union(queryRDD)
  }
  tempRDD = tempRDD.reduceByKey(_+_).sortBy(x => -x._2)
  println("\n\nThe returning result for '" + keyword + "' is the following: ")
  tempRDD.collect.foreach{ i =>
    println("Document ID: " + i._1 + " with frequency of " + i._2)
  }

}

def theNumber(){

  val keyword = readLine("What is the word you would like to get total number of occurences for? (Please en$


    val queryRDD = wordRDD.filter(_._1.equals(keyword)).map(x => (x._1, x._3))

    val tempRDD = queryRDD.reduceByKey(_+_)
    println("\n\nThe returning result for the total number of occurences of'" + keyword + "' is the following$


      println("\nThe Word: " + keyword + " with frequency of " + tempRDD.lookup(keyword))

    }

    def BigData(){
      wordRDD.collect.foreach{i =>
        println(i._1 + " -> " + i._2 + ": " + i._3)
      }
    }

    var files = sc.wholeTextFiles("hdfs://had6110.cs.pitt.edu:8020/user/chatree/CS1699/Books/*")
    val a = files.keys
    for(name <- a.collect()){
      val orig_text = files.lookup(name)
      val mapped = sc.parallelize(orig_text)
      val returnText = mapped.flatMap(l => l.split(' '))
      .map(_.toLowerCase())
      .map(word => word.filter(Character.isLetter(_)))
      .map(word => (word,1))
      .reduceByKey(_+_)
      val fname = name.split("/").last
      val toBeAdded : RDD[(String, String, Double)] = returnText.map(word => (word._1, fname, word._2))
      wordRDD = wordRDD.union(toBeAdded)

    }
    var keyword : String = "regular"
    while(keyword.equals("regular")){
      println(keyword)
      getIndex()
      keyword = readLine("\nEnter 'regular' to go again with the word search, 'advanced' to run our advan$
        while(keyword.equals("advanced")){
          println("\n1.) Get All the Words, the document they appear in, and the specific document fr$
          println("\n2.) Get The frequency of a specific word for all the documents. Enter 'theNumber$
          var enterText = readLine("\nEnter the option here: ")
          if(enterText.equals("BigData")){
            BigData()
          }else if(enterText.equals("theNumber")){
            theNumber()
          }
          keyword = readLine("\nEnter 'regular' to do just a word search, 'advanced' to do another ad$

          }
        }
        
