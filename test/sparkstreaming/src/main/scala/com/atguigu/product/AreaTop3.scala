/*
 * Copyright (c) 2020. Atguigu Inc. All Rights Reserved.
 * Date: 2020/4/15 下午1:37.
 * Author: HASEE.
 */

package com.atguigu.product

import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.ParamUtils
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object AreaTop {



  def main(args: Array[String]): Unit = {
     val jsonObject: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
     val taskParam: JSONObject = JSONObject.fromObject(jsonObject)
     val taskUuid: String = UUID.randomUUID().toString
     val sparkConf: SparkConf = new SparkConf().setAppName("top3").setMaster("local[*]")
     val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val cityid2clickActionRDD = getCityAndProductInfo(sparkSession,taskParam)

    // 获取任务日期参数
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)
    //查詢城市信息
    val cityid2cityInfoRDD = getcityid2CityInfoRDD(sparkSession)

    // 生成点击商品基础信息临时表
    // 将点击行为cityid2clickActionRDD和城市信息cityid2cityInfoRDD进行Join关联
    // tmp_click_product_basic

    generateTempClickProductBasicTable(sparkSession, cityid2clickActionRDD, cityid2cityInfoRDD)
  }


  def generateTempClickProductBasicTable(spark: SparkSession, cityid2clickActionRDD: RDD[(Long, Row)], cityid2cityInfoRDD: RDD[(Long, Row)])= {
  val joinRDD = cityid2clickActionRDD.join(cityid2cityInfoRDD)
    val unit = joinRDD.map {
      case (cityid, (action, cityinfo)) => {
        val productid = action.getLong(1)
        val cityName = cityinfo.getString(1)
        val area = cityinfo.getString(2)
        (cityid, cityName, area, productid)
      }
    }
    import  spark.implicits._
    val frame = unit.toDF("city_id", "city_name", "area", "product_id")
    frame.createOrReplaceTempView("tmp_click_product_basic")
    spark.sql("select * from tmp_click_product_basic").show()
  }

  //查詢城市信息
  def getcityid2CityInfoRDD(spark: SparkSession) = {
    val cityInfo = Array((0L, "北京", "华北"), (1L, "上海", "华东"), (2L, "南京", "华东"), (3L, "广州", "华南"), (4L, "三亚", "华南"), (5L, "武汉", "华中"), (6L, "长沙", "华中"), (7L, "西安", "西北"), (8L, "成都", "西南"), (9L, "哈尔滨", "东北"))
    import spark.implicits._
    val frame = spark.sparkContext.makeRDD(cityInfo).toDF("city_id", "city_name", "area")
    frame.rdd.map(item=>(item.getAs[Long]("city_id"),item))
  }


  def getCityAndProductInfo(sparkSession: SparkSession, taskParam: JSONObject)= {
     var startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
     var endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)
    val sql =
      "SELECT " +
        "city_id," +
        "click_product_id " +
        "FROM user_visit_action " +
        "WHERE click_product_id IS NOT NULL and click_product_id != -1L " +
        "AND date>='" + startDate + "' " +
        "AND date<='" + endDate + "'"
    val frame = sparkSession.sql(sql)
    frame.rdd.map(item=>(item.getAs[Long]("city_id"),item))
  }


}


