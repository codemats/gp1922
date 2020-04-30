package spark.day07

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


//DataFrame---->DataSet // frema.as[对象]
object DataFrame2DataSetDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val filePathEmployees = "/Users/a10.12/Documents/study/bigdata/spark/gp1922/testFile/people.json"

    val dataFrame: DataFrame = spark.read.json(filePathEmployees)

    import spark.implicits._
    //frema.as[对象]
    val dataSet: Dataset[PersonDaset] = dataFrame.as[PersonDaset]
    dataSet.show()

    //dataset 转 dataFrame
    dataSet.toDF()

    spark.stop()
  }
}

case class PersonDaset(name:String,age:BigInt)
