package lib

import java.lang.System._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import akka.actor._

import scala.collection.mutable.ArrayBuffer
import com.redislabs.provider.redis._

/**
  * Created by xrh on 2016/12/14.
  */


class Master_deprecated(system:ActorSystem,sc : SparkContext, redisSliceNum : Int,storageSliceNum : Int) extends Actor
{
  private var backetsNum=0
  private var taskProcessed = 0
  private var fileSender: Option[ActorRef] = None

  def receive = {

    case  StartMsg(ccd_num: Int,keysHash:Array[ArrayBuffer[String]],outputSubDir:String) =>
      backetsNum=keysHash.length
      fileSender = Some(sender)
      var st=0
      (0 until backetsNum).foreach
      {
        backet =>
          if (keysHash(backet).nonEmpty)
          {
            val listRDD = sc.fromRedisList(keysHash(backet).toArray,redisSliceNum)
            val outputSubSubDir = s"$outputSubDir/$backet"
            st=st+1

            /***parallel store******/
            val storeTh = system.actorOf(Props(new store_thread_deprecated(sc,st,storageSliceNum)).withDispatcher("my-thread-pool-dispatcher"))
            println("start thread "+st)
            // storeTh.start()
            storeTh ! storeInfo(outputSubSubDir, listRDD)
            /**********************/
          }
      }

      case  TaskSuccessMsg(words: String) =>
        taskProcessed+=1
        if(taskProcessed==backetsNum)
        {
          fileSender.get ! TaskSuccessMsg("ccd finish!")
        }

  }


}


class store_thread_deprecated(sc : SparkContext, storeTime: Int,coalesce_slice:Int) extends Actor
{
  val ssc = new store_single_CCD(sc)
  def receive = {
    case storeInfo(outputPath: String, storeRDD: RDD[String]) =>
      ssc.store(outputPath, storeRDD,coalesce_slice)
      println(s"storing data successfully, $storeTime times")
      sender ! TaskSuccessMsg("success")
    // context.stop(self)

  }


}


class Master(system:ActorSystem,sc : SparkContext, redisSliceNum : Int,storageSliceNum : Int,Collecter:ActorRef,maxLength_backet:Int) extends Actor
{
  private var backetsNum=0
  private var taskProcessed = 0
 // private var fileSender: Option[ActorRef] = None

  def receive = {

    case  backetInfo(outputPath : String, arrayStars : Array[String]) =>

            backetsNum+=1
            /***parallel store******/
            val storeTh = system.actorOf(Props(new store_thread(sc,redisSliceNum,storageSliceNum,Collecter,maxLength_backet)).withDispatcher("my-thread-pool-dispatcher"))
            println("start thread "+backetsNum)
            // storeTh.start()
            storeTh ! backetInfo_worker(outputPath, arrayStars,backetsNum)
            /**********************/

      }
}

class Collecter(system:ActorSystem) extends Actor
{
  private var backetsNum=1000000//danger!

  private var taskProcessed = 0
  private var MainSender: Option[ActorRef] = None

  def receive = {

    case  TaskSuccessMsg(words: String) =>
      taskProcessed+=1

      if(taskProcessed>=backetsNum)
      {
        println("check:success process num "+taskProcessed)
        println("check:bucket num "+backetsNum)
        MainSender.get ! TaskSuccessMsg("All backets finish!")
      }

    case  AnyInt(x: Int) =>
      backetsNum=x
      MainSender = Some(sender)

  }
}


                                      //the para which is permanent
class store_thread(sc : SparkContext,redisSliceNum : Int,coalesce_slice:Int,Collecter:ActorRef,maxLength_backet:Int) extends Actor
{
  //val ssc = new store_single_CCD(sc)
  def receive = {
    case  backetInfo_worker(outputPath : String, arrayStars : Array[String],backetsNum:Int)=>

    if(arrayStars.length<=maxLength_backet)
    {
      val storeRDD = sc.fromRedisList(arrayStars, redisSliceNum)

      store_single_CCD(sc).store(outputPath, storeRDD, coalesce_slice)

      println(s"storing data successfully, $backetsNum times")

      Collecter ! TaskSuccessMsg("s")

       context.stop(self)
    }
    else
    {
      val step=arrayStars.length/maxLength_backet
      val steplength=arrayStars.length/step
      println("start serial storing data; step num: "+step)

      for(time<-0 to step-1)
      {

        if(time<=step-2)
        {
          val arr = new Array[String](steplength)
          System.arraycopy(arrayStars, time*steplength, arr, 0, steplength)
          println("stars num in the slice" + time + " :" + arr.length)

          // arr.foreach(println)
          val storeRDD = sc.fromRedisList(arr, redisSliceNum)

          store_single_CCD(sc).store(outputPath, storeRDD, coalesce_slice)

          println(s"serial storing data, $time times")

        }
        else
        {
          val arr = new Array[String](arrayStars.length-time*steplength)
          System.arraycopy(arrayStars, time*steplength, arr, 0, arrayStars.length-time*steplength)
          println("stars num in the slice" + time + " :" + arr.length)

          // arr.foreach(println)
          val storeRDD = sc.fromRedisList(arr, redisSliceNum)

          store_single_CCD(sc).store(outputPath, storeRDD, coalesce_slice)

          println(s"serial storing data, $time times")
        }

      }
      println(s"storing data successfully, $backetsNum times")

      Collecter ! TaskSuccessMsg("s")

    }


  }


}

