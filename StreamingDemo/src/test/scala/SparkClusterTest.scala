import org.apache.spark.sql.SparkSession

object SparkClusterTest {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkClusterTest")
      .master("spark://hadoop102:7077") // ← 你的 Spark Master 地址
      // 如果 Driver 在 Windows，需要显式指定 IP 和端口
      .config("spark.driver.host", "192.168.10.1") // ← 改成你的 Windows 主机 IP
      .config("spark.driver.port", "9999")          // ← 随便一个空闲端口
      .getOrCreate()

    println("✅ SparkSession created successfully")

    // 创建一个简单的 RDD 来触发任务分配
//    val data = spark.sparkContext.parallelize(1 to 100, 4)
//    val sum = data.map(_ * 2).reduce(_ + _)

    // 避免直接使用lambda表达式
    // 避免直接使用lambda表达式
    val data = spark.sparkContext.parallelize(1 to 100, 4)

    // 使用明确的方法引用或函数定义
    def multiplyByTwo(x: Int): Int = x * 2
    val doubled = data.map(multiplyByTwo)

    def add(a: Int, b: Int): Int = a + b
    val sum = doubled.reduce(add)

    println(s"✅ Spark Cluster Test OK! Sum result = $sum")

    spark.stop()
  }
}
