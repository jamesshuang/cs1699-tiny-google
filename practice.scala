import java.io._
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.hadoop.fs.{FileSystem,Path}

@SerialVersionUID(100L)
class payload extends Serializable{

  var frequency: Double = 0
  var document_id: String = ""
  def this(count:Int, path:String){
    this()
    this.frequency = count
    this.document_id = path
  }

  def printer(){
    println(frequency)
    println(document_id)
  }
}



//Inneficiencies:
  //Was not having global var final, instead was just groupingBy when person looking
  //This increase the time taken incrementally.

@SerialVersionUID(100L)
class invertedIndex extends Serializable{
  import java.io._

  var grouped : List[(String,payload)] = List()

  def init(){
      add()
  }

  def add(){
    var files = sc.wholeTextFiles("hdfs://localhost:9000/user/nick/1699/input/*")
    val a = files.keys
    for(name <- a.collect()){
      val orig_text = files.lookup(name)
      val mapped = sc.parallelize(orig_text)
      val returnText = mapped.flatMap(l => l.split(' '))
      .map(_.toLowerCase())
      .map(word => word.filter(Character.isLetter(_)))
      .map(word => (word,1))
      .reduceByKey(_+_)
      .map(word => (word._1, new payload(word._2, name)))
      this.grouped = this.grouped ++ returnText.collect().toList
    }
  }


  def returnList(word: String){
    val merge = grouped.groupBy(_._1)
    println(merge(word))
  }
}


  //def

  //val merge = grouped.groupBy(_._1)
  //println(merge("the"))
var ii = new invertedIndex()
ii.init

//val name = readLine("What's your name? ")

ii.returnList("the")
