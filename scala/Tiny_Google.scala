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

  val keyword = readLine("\nWhat is the word you would like to get total number of occurences for? (Please enter only 1 word): ").toString
  
  
  val queryRDD = wordRDD.filter(_._1.equals(keyword)).map(x => (x._1, x._3))
  
  val tempRDD = queryRDD.reduceByKey(_+_)
  println("\n\nThe returning result for the total number of occurences of'" + keyword + "' is the following: ")
  
  
  println("\nThe Word: " + keyword + " with frequency of " + tempRDD.lookup(keyword))
  
}

def BigData(){
	wordRDD.collect.foreach{i => 
		println(i._1 + " -> " + i._2 + ": " + i._3)
	}
}

def bigNumber(){
	val keyword = readLine("\nWhat is the word you would like to get total number of occurences for? (Please enter only 1 word): ").toString

  
	val tempRDD = wordRDD.filter(_._1.equals(keyword)).map(x => (x._1, x._3)).reduceByKey(_+_)

	val sum = wordRDD.map(_._3).reduce(_+_)
	
	val frequency = (tempRDD.lookup(keyword)(0)/sum) * 100

	println("\nThe total percentage that this word appears in all documents compared to all other words is: " + frequency)
}

def smallNumber(files: RDD[String]){
	var count : Int = 0
	val withIndex = files.zipWithIndex
	val indexKey = withIndex.map{case (k,v) => (v,k)}
	val d = indexKey.keys
	for(fname <- d.collect()){
		val cur_fname = indexKey.lookup(fname).mkString("").split("/").last
		println("\nEnter (" + fname + ") for filename: " + cur_fname)
	}
	println("\nEnter the corresponding number here: ")
	val number = readInt()
	val file2b = indexKey.lookup(number).mkString("").split("/").last

	val totalFromDoc = wordRDD.filter(_._2.equals(file2b)).map(_._3).reduce(_+_)
	val keyword = readLine("\nWhat is the word you would like to get total number of occurences for? (Please enter only 1 word): ").toString

  	val keyFromDoc = wordRDD.filter(_._1.equals(keyword)).filter(_._2.equals(file2b)).map(_._3).reduce(_+_)

        val frequency = (keyFromDoc/totalFromDoc) * 100

        println("\nThe total percentage that this word appears in the document " + file2b + " is: " + frequency)	


}
println("\nWelcome to Tiny Google! The Files are being loaded in!\n\n")

	var files = sc.wholeTextFiles("hdfs://had6110.cs.pitt.edu:8020/user/chatree/CS1699/Books/*")
	var a = files.keys
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
	files.unpersist()	

println("\nWelcome to Tiny Google! The Files have been loaded in!!\n\n")	

var keyword : String = "regular"
while(keyword.equals("regular")){
	getIndex()
	keyword = readLine("\nEnter 'regular' to go again with the word search, 'advanced' to run our advanced queries, anything else to stop: ")
	while(keyword.equals("advanced")){
		println("\n1.) Get All the Words, the document they appear in, and the specific document frequency. Enter 'BigData'")
		println("\n2.) Get The frequency of a specific word for all the documents. Enter 'theNumber'")	
		println("\n3.) Get the percentage of a word to in all documents to all words in all documents. Enter 'bigNumber'")
		println("\n4.) Get the percentage of a word in a document to all word in the ONE document. Enter 'smallNumber'")
		var enterText = readLine("\nEnter the option here: ")
		if(enterText.equals("BigData")){
			BigData()

		}else if(enterText.equals("theNumber")){
			theNumber()

		}else if(enterText.equals("bigNumber")){
			bigNumber()

		}else if(enterText.equals("smallNumber")){
			smallNumber(a)
			
		}
		keyword = readLine("\nEnter 'regular' to do just a word search, 'advanced' to do another advanced query, or anything else to stop: ")

	}
}

