/**
 * Created by root on 12/4/16.
 */

import akka.actor.{Props, ActorSystem}
import lib._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object astroDB_store {


  def main(args: Array[String]): Unit = {


    if(args(0)== "-h")
    {
      println("parameter 0: redis: ip:port")
      println("parameter 1: output path")
      println("parameter 2: redis slice number of RDDs")
      println("parameter 3: pattern of finding from Redis e.g., ref")
      println("parameter 4: the total number of CCDs")
      println("parameter 5: the number of buckets in HDFS")
      println("parameter 6: the storing length of each bucket")
      println("parameter 6: the storage slice number of rdds")
      println("parameter 7: the max num of each bucket")
      System.exit(1)
    }

    var redisHost = new String
    var outputPath = new String
    var redisSliceNum= 0
    var storageSliceNum = 0
    var starPrefix = new String
    var ccdNum = 0
    var starClusterNum = 0
    var scWriteLength = 0
    var maxLength_backet=0


    for(a <- args.indices)
    {
      if (args(a).head == '-')
      {
        args(a) match {
          case "-scWriteLength" => scWriteLength = args(a+1).toInt  //sc means star cluster
          case "-outputPath"    => outputPath = args(a+1)
          case "-ccdNum"    => ccdNum = args(a+1).toInt
          case "-redisSliceNum"  => redisSliceNum = args(a+1).toInt
          case "-storageSliceNum"  => storageSliceNum = args(a+1).toInt
          case "-starPrefix" => starPrefix = args(a+1)
          case "-starClusterNum" => starClusterNum = args(a+1).toInt
          case "-redisHost" => redisHost = args(a+1)
          case "-maxbucket" => maxLength_backet = args(a+1).toInt
          case _ => sys.error(s"${args(a)} is illegal")
        }
      }
    }



    val redisIP = redisHost.split(":")(0)
    val redisPort = redisHost.split(":")(1)
    val conf = new SparkConf()
               .setAppName("redis_store")
               .set("redis.host", redisIP)
               .set("redis.port", redisPort)
               .set("spark.scheduler.mode", "FAIR")
               // .set("spark.executor.extraJavaOptions"," -Dcom.sun.management.jmxremote.port=8889 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false")
//              .setMaster("local[*]")
    val sc = new SparkContext(conf)
    var storeTime = 0
//    val sqlContext = new SQLContext(sc)


    import com.typesafe.config.ConfigFactory //
    val customConf = ConfigFactory.parseString(
      """
        my-thread-pool-dispatcher {
          # Dispatcher是基于事件的派发器的名称
          type = Dispatcher
          # 使用何种 ExecutionService
          executor = "thread-pool-executor"
          # 配置线程池
          thread-pool-executor {
            # 容纳基于因子的内核数的线程数下限
            core-pool-size-min = 2
            # 内核线程数 .. ceil(可用CPU数＊倍数）i7-4790 4 cores
            core-pool-size-factor = 2.0
            # 容纳基于倍数的并行数量的线程数上限
            core-pool-size-max = 10
          }
          # Throughput 定义了线程切换到下一个actor之前处理的消息数上限
          # 设置成1表示尽可能公平.
         throughput = 1
        }

      """)

    val system = ActorSystem("System", ConfigFactory.load(customConf))

    val Monitor=system.actorOf(Props(new Collecter(system)))
    val masterActor=system.actorOf(Props(new Master(system,sc,redisSliceNum,storageSliceNum,Monitor,maxLength_backet)))


    for (i <- 1 to ccdNum)
      {
         val ssc =  store_single_CCD(sc)
//         storeTime = ssc.createModHash(backetsNum,perBacketLong,i.toString, sliceNum, starPrefix,outputPath, storeTime)
        storeTime=ssc.createModHashFromRedis(masterActor,system,starClusterNum,scWriteLength,i.toString, redisSliceNum,storageSliceNum, starPrefix,outputPath, storeTime)
      }

    val all_backetsNum=ccdNum*starClusterNum
    println("the sum of backets is "+all_backetsNum)

    import akka.util.Timeout
    import scala.concurrent.duration._
    import akka.pattern.ask
    implicit val timeout = Timeout(6000 seconds)//danger!

    val future=Monitor ? AnyInt(all_backetsNum)
    import scala.concurrent.ExecutionContext.Implicits.global

    future.map { (result) =>
      println(Thread.currentThread().getName())
      println(result)
      system.shutdown
      println("All thread complete!")
    }

    println(Thread.currentThread().getName())
    println("Main thread complete!")

  }
}