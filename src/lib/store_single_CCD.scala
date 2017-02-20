package lib

import akka.actor.{ActorRef, Props, ActorSystem}


import org.apache.spark.SparkContext
import com.redislabs.provider.redis._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer
/**
  * Created by xrh on 2016/12/12.
  */
case class store_single_CCD(sc : SparkContext)
{

//  def createModHash(backetsNum : Int, perBacketLong : Int,CCDNum : String,
//                    sliceNum : Int, starPrefix : String,
//                    outputPath : String,
//                    storeTime : Int): Int =
//  // when the perBacketLong is very large ,e.g.,1000, the programme will be error because java.lang.StackOverflowError
//  {
//    var st = storeTime
//    val keypattern = s"${starPrefix}_${CCDNum}_*"
//    val starKeys = sc.fromRedisKeyPattern(keypattern,sliceNum).collect()
//
//    val num_star=starKeys.length
//    println(num_star)
//
//    if (starKeys == null)
//      {
//        sys.error("Key list is null")
//      }
//    val outputHashData = new Array[RDD[String]](backetsNum) //initialize to  null
//    val outputHashNum = new Array[Int](backetsNum)
//    val outputSubDir = s"$outputPath/$CCDNum"
//    starKeys.foreach
//    {
//      star =>
//       val listRDD = sc.fromRedisList(star,sliceNum)
//
//       val starNum = star.split("_")(2).toInt
//       val hashIndex = starNum % backetsNum
//
//       val  outputDir=outputSubDir+s"/$hashIndex"
//
//        val keyStarRDD=listRDD.map(a => s"$star $a")
//
//        println("hashIndex "+ hashIndex+" outputHashNum "+outputHashNum(hashIndex))
//        if(outputHashNum(hashIndex)==0)// first time in the  outputHashData(hashIndex) is null
//        {
//          outputHashData(hashIndex)=keyStarRDD
//
//          outputHashNum(hashIndex) = outputHashNum(hashIndex) + 1
//        }
//        else {
//          val tmp=outputHashData(hashIndex).union(keyStarRDD)//.coalesce(sliceNum,false)
//          outputHashData(hashIndex)= tmp
//          outputHashNum(hashIndex) = outputHashNum(hashIndex) + 1
//
//          if (outputHashNum(hashIndex) > perBacketLong)
//          {
//            st=st+1
////            store(outputPath, outputHashData(hashIndex) ,sliceNum) // serial store
//            //parallel store
//            val storeTh = new store_thread(sc,st,sliceNum)
//            println("start thread "+st)
//            storeTh.start()
//            storeTh ! storeInfo(outputDir, outputHashData(hashIndex))
//            //parallel store
//            outputHashNum(hashIndex) = 0
//
//          }
//        }
//    }
//
//    for(hashIndex <- 0 to backetsNum)
//      {
//        if(outputHashNum(hashIndex)!=0) {
//          val outputDir = outputSubDir + s"/$hashIndex"
//          st = st + 1
//          val storeTh = new store_thread(sc, st,sliceNum)
//          println("start thread " + st)
//          storeTh.start()
//          storeTh ! storeInfo(outputDir, outputHashData(hashIndex))
//        }
//        else {
//          println("Empty in final backet "+hashIndex)
//        }
//      }
//
//      st
//  }


//  def createModHashFromRedis_deprecated(system:ActorSystem,backetsNum : Int, perBacketLong : Int,CCDNum : String,
//                             redisSliceNum : Int,storageSliceNum : Int,
//                             starPrefix : String, outputPath : String,
//                             storeTime : Int): Int =
//  {
//    var st = storeTime
//    val keypattern = s"${starPrefix}_${CCDNum}_*"
//    val starKeys = sc.fromRedisKeyPattern(keypattern,redisSliceNum).collect()
//
//    val num_star=starKeys.length
//    println(num_star)
//   if (starKeys == null)
//    {
//      sys.error("Key list is null")
//    }
//    val keysHash = new Array[ArrayBuffer[String]](backetsNum) //initialize to  null
//    (0 until backetsNum).foreach(keysHash(_) = new ArrayBuffer[String])
//
//    val outputSubDir = s"$outputPath/$CCDNum"
//    starKeys.foreach {
//      star =>
//        val starNum = star.split("_")(2).toInt
//        val hashIndex = starNum % backetsNum
//        keysHash(hashIndex)+=star
//    }
//
//
//    val masterActor=system.actorOf(Props(new Master(system,sc,redisSliceNum,storageSliceNum)))
//
//    import akka.util.Timeout
//    import scala.concurrent.duration._
//    import akka.pattern.ask
//    implicit val timeout = Timeout(60 seconds)
//
//    val future = masterActor .? ( StartMsg(1,keysHash,outputSubDir))
//
//    import scala.concurrent.ExecutionContext.Implicits.global
//
//    future.map { result =>
//      println(Thread.currentThread().getName())
//      println(result)
//      system.shutdown
//    }
//
////    (0 until backetsNum).foreach
////    {
////      backet =>
////        if (keysHash(backet).nonEmpty)
////          {
////            val listRDD = sc.fromRedisList(keysHash(backet).toArray,redisSliceNum)
////            val outputSubSubDir = s"$outputSubDir/$backet"
////            st=st+1
////
//////           store(outputPath, listRDD ,storageSliceNum) // serial store
////
////
////            /***parallel store******/
////            val storeTh = system.actorOf(Props(new store_thread(sc,st,storageSliceNum)).withDispatcher("my-thread-pool-dispatcher"))
////            println("start thread "+st)
////           // storeTh.start()
////            storeTh ! storeInfo(outputSubSubDir, listRDD)
////            /**********************/
////           }
////    }
//
//    st
//
//  }


  def createModHashFromRedis(masterActor:ActorRef,system:ActorSystem,backetsNum : Int, perBacketLong : Int,CCDNum : String,
                                        redisSliceNum : Int,storageSliceNum : Int,
                                        starPrefix : String, outputPath : String,
                                        storeTime : Int): Int =
  {
    var st = storeTime
    val keypattern = s"${starPrefix}_${CCDNum}_*"
    val starKeys = sc.fromRedisKeyPattern(keypattern,redisSliceNum).collect()

    val num_star=starKeys.length
    println("whole num of stars in one CCD "+num_star)

    if (starKeys == null)
    {
      sys.error("Key list is null")
    }
    val keysHash = new Array[ArrayBuffer[String]](backetsNum) //initialize to  null
    (0 until backetsNum).foreach(keysHash(_) = new ArrayBuffer[String])

    val outputSubDir = s"$outputPath/$CCDNum"
    starKeys.foreach {
      star =>
        val starNum = star.split("_")(2).toInt
        val hashIndex = starNum % backetsNum
        keysHash(hashIndex)+=star
    }


    (0 until backetsNum).foreach
    {
      backet =>
        if (keysHash(backet).nonEmpty)
        {
          val arrayStars =keysHash(backet).toArray
          val outputSubSubDir = s"$outputSubDir/$backet"
          st=st+1
          masterActor ! backetInfo(outputSubSubDir, arrayStars)
         }
    }

    st

  }


  def store(outputPath : String, storeRDD : RDD[String],coalesce_slice:Int): Unit =
  {
    val sqlContext = new SQLContext(sc)
    val tableStruct =
      StructType(Array(
        StructField("star_id",StringType,true),
        StructField("ccd_num",IntegerType,true),
        StructField("imageid",IntegerType,true),
        StructField("zone",IntegerType,true),
        StructField("ra",DoubleType,true),
        StructField("dec",DoubleType,true),
        StructField("mag",DoubleType,true),
        StructField("x_pix",DoubleType,true),
        StructField("y_pix",DoubleType,true),
        StructField("ra_err",DoubleType,true),
        StructField("dec_err",DoubleType,true),
        StructField("x",DoubleType,true),
        StructField("y",DoubleType,true),
        StructField("z",DoubleType,true),
        StructField("flux",DoubleType,true),
        StructField("flux_err",DoubleType,true),
        StructField("normmag",DoubleType,true),
        StructField("flag",DoubleType,true),
        StructField("background",DoubleType,true),
        StructField("threshold",DoubleType,true),
        StructField("mag_err",DoubleType,true),
        StructField("ellipticity",DoubleType,true),
        StructField("class_star",DoubleType,true),
        StructField("orig_catid",IntegerType,true),
        StructField("timestamp",IntegerType,true)
))

    val storeRDDRow=storeRDD.coalesce(coalesce_slice,false).map(str=>str.split(" "))
      .map{ p=> //p is one line
        Row(
          if(p(0)==null) null else p(0),
          if(p(1)==null) null else p(1).toInt,
          if(p(2)==null) null else p(2).toInt,
          if(p(3)==null) null else p(3).toInt,
          if(p(4)==null) null else p(4).toDouble,
          if(p(5)==null) null else p(5).toDouble,
          if(p(6)==null) null else p(6).toDouble,
          if(p(7)==null) null else p(7).toDouble,
          if(p(8)==null) null else p(8).toDouble,
          if(p(9)==null) null else p(9).toDouble,
          if(p(10)==null) null else p(10).toDouble,
          if(p(11)==null) null else p(11).toDouble,
          if(p(12)==null) null else p(12).toDouble,
          if(p(13)==null) null else p(13).toDouble,
          if(p(14)==null) null else p(14).toDouble,
          if(p(15)==null) null else p(15).toDouble,
          if(p(16)==null) null else p(16).toDouble,
          if(p(17)==null) null else p(17).toDouble,
          if(p(18)==null) null else p(18).toDouble,
          if(p(19)==null) null else p(19).toDouble,
          if(p(20)==null) null else p(20).toDouble,
          if(p(21)==null) null else p(21).toDouble,
          if(p(22)==null) null else p(22).toDouble,
          if(p(23)==null) null else p(23).toInt,
          if(p(24)==null) null else p(24).toInt)
          }

    println( "Thread " + Thread.currentThread().getName() )
//    val df=sqlContext.createDataFrame(storeRDDRow,tableStruct)

    sqlContext.createDataFrame(storeRDDRow,tableStruct).write.mode("append").save(outputPath)
  }

}
