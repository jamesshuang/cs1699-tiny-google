import java.io._
import org.apache.commons.io.FileUtils
import org.apache.spark.SparkContext._

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
    val files = getListOfFiles("input/").filter(_.getName.endsWith(".txt"))
    for (name <- files) {
      add(name)
    }
  }

  def add(name: java.io.File){
    var mapped = sc.textFile("./" + name)
    .flatMap(l => l.split(' '))
    .map(_.toLowerCase())
    .map(word => word.filter(Character.isLetter(_)))
    .map(word => (word,1))
    .reduceByKey(_+_).collect
    .map(word => (word._1, new payload(word._2, name.getName)))
    this.grouped = this.grouped ++ mapped
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
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
ii.returnList("the")
