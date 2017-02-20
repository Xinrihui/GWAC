package lib

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


case class storeInfo(outputPath : String, storeRDD : RDD[String])
//case class getperfdata(slavesHostname : String,localDir : String,remoteDir : String)
case class kill_thread(info : String)


case class TaskSuccessMsg(words: String)
case class AnyInt(x: Int)

case class StartMsg(ccd_num: Int,keysHash:Array[ArrayBuffer[String]],outputSubDir:String)

case class backetInfo(outputPath : String, arrayStars : Array[String])

case class backetInfo_worker(outputPath : String, arrayStars : Array[String],backetsNum:Int)