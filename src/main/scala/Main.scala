import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import sesion4.SparkUtils.runSparkSession

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
object Main {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = runSparkSession("Test")
    val rdd: RDD[String] = spark.sparkContext.parallelize(Seq("a","b","c"))
    println(rdd.collect().mkString("Array(", ", ", ")"))

    spark.read.csv("G:\\Projects\\bd-processing16\\src\\main\\scala\\examen_estructura\\ventas.csv").show(false)

    println("Hello World")

  }
}

