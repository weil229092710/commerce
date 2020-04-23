import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc = new SparkContext(conf)
    val rdd1 = sc.makeRDD((1 to 20))
    val r01 = rdd1.map ( _*2 ).filter(_<10)
    val rdd = sc.makeRDD(List("hello","wor"))
    println("======flatMap操作======")

    println(r01.collect().mkString("|"))
    println("======flatMap操作======")
    val strings = Array("one", "two", "two", "three", "three", "three")
    val value: RDD[(String, Iterable[Int])] = sc.parallelize(strings).map(word => (word, 1)).groupByKey()

  }

}
