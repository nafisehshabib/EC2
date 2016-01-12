
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random


/**
  * Created by nafiseh.shabib on 12/01/2016.
  */



  object GenerateNames {
    val SPARK_MASTER = "spark://ec2-52-48-90-101.eu-west-1.compute.amazonaws.com:7070"
    val HDFS = "hdfs://ec2-52-48-90-101.eu-west-1.compute.amazonaws.com:9000"
    val outputDir = HDFS + "/output/part"

    def main(args: Array[String]) {
      val conf = new SparkConf()
        .setMaster(SPARK_MASTER)
        .setAppName("GenerateNames")
      val sc = new SparkContext(conf)
      for (partition <- 0 to 3) {
        val data = Seq.fill(1000000)(Random.alphanumeric.take(5).mkString)
        sc.parallelize(data, 1).saveAsTextFile(outputDir + "_" + partition)
      }
    }

  }




