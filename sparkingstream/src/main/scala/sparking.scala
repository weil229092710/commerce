import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

object  sparking {
  def main(args: Array[String]): Unit = {
    ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val conf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    new   StreamingContext(sparkSession.sparkContext,Seconds(5))
  }

}
