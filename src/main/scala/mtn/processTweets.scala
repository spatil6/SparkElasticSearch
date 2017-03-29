package mtn

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.elasticsearch.spark._
import java.util.Date
///
object processTweets {
  case class Person(first_name: String, last_name: String, age: Int)
  //case class Social(id: Long, name: String, post: String, posturl: String, mediaTye: String, published_date: String, zensocial_sentiment: Int, zensocial_kloutscore: Double, imageUrl: String, zensocial_classification: String, retweet: Int, favorite: String, socialId: Int)
  case class Social(id: String,  post: String, posturl: String, mediaTye: String, publisheddate: String, zensocialsentiment: Int, zensocialkloutscore: Double, imageUrl: String, zensocialclassification: String, retweet: Int, favorite: Int, socialId: String)
  val csvFormat = "com.databricks.spark.csv"
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Input file <file path> ")
      System.exit(1)
    }
    val _sparkConf = new SparkConf().setAppName("MTN")
    _sparkConf.set("es.nodes", "35.160.140.182:9350")

    val sc = new SparkContext(_sparkConf);
    val p = sc.textFile("file:///hadoopdata//usr//local//temp1//MTN.csv")
    val format = new java.text.SimpleDateFormat("dd-MM-yyyy hh:mm")
    println(format.parse("18-10-2016 05:26"))
   
    //hdfs://hadoopdata/usr/local/MTN.txt
    val pmap = p.map(line => line.split(","))
    val personRDD = pmap.map(p => Social(p(0), p(1), p(2), p(3), p(4), p(5).toInt, p(6).toDouble, p(7), p(8), p(9).toInt, p(10).toInt, p(11)))
    personRDD.map(x => {
      Map(
        "id" -> x.id,
       // "name" -> x.name,
        "post" -> x.post,
        "posturl" -> x.posturl,
        "mediatype" -> x.mediaTye,
        "publisheddate" -> format.parse(x.publisheddate),
        "zensocialsentiment" -> x.zensocialsentiment,
        "zensocialklout-score" -> x.zensocialkloutscore,
        "imageurl" -> x.imageUrl,
        "zensocialclassification" -> x.zensocialclassification,
        "retweet" -> x.retweet,
        "favorite" -> x.favorite,
        "socialid" -> x.socialId)
    }).saveToEs("socials/tweers");

  }
}