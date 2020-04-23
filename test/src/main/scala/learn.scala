import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object learn {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("score").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    val scoreDF = sparkSession.sparkContext.makeRDD(
      Array(Score("a1", 1, 80),
      Score("a2", 1, 78),
      Score("a3", 1, 95),
      Score("a4", 2, 74),
      Score("a5", 2, 92),
      Score("a6", 3, 99),
      Score("a7", 3, 99),
      Score("a8", 3, 45),
      Score("a9", 3, 55),
      Score("a10", 3, 78))
    ).toDF("name", "clas", "score")
      scoreDF.createOrReplaceTempView("score")
      //scoreDF.show()
      sparkSession.sql("select name, clas, score, dense_rank() over(order by score) name_count from score").show()
      }
}
case class Score(name:String,clas:Int,score:Int)