import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Main class for the application.
 *
 * @author zhao kai
 * @version 1.0
 * @since 1.0
 */
object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark POC")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val list = List.fill(500)(Random.nextInt(10))
    val listrdd = sc.parallelize(list, 5)
    val pairs = listrdd.map(x => (x, x*x))
    val reduced = pairs.reduceByKey((v1, v2)=>v1+v2)
    val finalrdd = reduced.mapPartitions(iter => iter.map({case(k,v)=>"K="+k+",V="+v}))

    finalrdd.collect()

    finalrdd.foreach(println)
  }
}
