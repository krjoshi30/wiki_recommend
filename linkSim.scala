import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.DataFrame

object linkSim {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext

    val lines = sc.textFile(args(0))

    // Turns link into K/V of (article, set with single link)
    val links = lines.map(s => (s.split("\t")(0), s.split("\t")
                                                    .toSet
                                                    .filter(_ != (s.split("\t")(0)))))

    // Reduces into (article, set of outlinks)
    val linksMap = links.reduceByKey(_.union(_))

    // Tuple ordered in (current article, dest. article, SimScore) 
    // SimScore is jaccard index based on sets of outbound links
    val cross = linksMap.cartesian(linksMap).map(s => (s._1._1, s._2._1, 
                                                        s._1._2.intersect(s._2._2).size.toDouble / 
                                                        s._1._2.union(s._2._2).size.toDouble))

    // Sets recommendation score of article to itself to 0
    val crossClean = cross.map(s => if (s._1 == s._2) (s._1, s._2, 0.0) else (s._1, s._2, s._3))

    // Moves to dataframe
    val dataframe = spark.createDataFrame(crossClean).toDF("article_name","article_name_2","distance")

    //dataframe.show()

    // Pivots dataframe into table
    val pivotdf = dataframe.groupBy("article_name").pivot("article_name_2").avg("distance")

    //pivotdf.show()

    // Writes to CSV
    pivotdf.coalesce(1).write.option("header","true").csv(args(2))

  }
}