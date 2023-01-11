package study
import org.apache.flink.streaming.api.scala._
object chapter02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env
      .fromElements(
        ("a", 1), ("a", 3), ("b", 3), ("b", 4)
      )
    stream.keyBy(_._1).sum(1).print() //对元组的索引 1 位置数据求和
    stream.keyBy(_._1).sum("_2").print() //对元组的第 2 个位置数据求和
//    stream.keyBy(_._1).max(1).print() //对元组的索引 1 位置求最大值
//    stream.keyBy(_._1).max("_2").print() //对元组的第 2 个位置数据求最大值
//    stream.keyBy(_._1).min(1).print() //对元组的索引 1 位置求最小值
//    stream.keyBy(_._1).min("_2").print() //对元组的第 2 个位置数据求最小值
//    stream.keyBy(_._1).maxBy(1).print() //对元组的索引 1 位置求最大值
//    stream.keyBy(_._1).maxBy("_2").print() //对元组的第 2 个位置数据求最大值
//    stream.keyBy(_._1).minBy(1).print() //对元组的索引 1 位置求最小值
//    stream.keyBy(_._1).minBy("_2").print() //对元组的第 2 个位置数据求最小值
    env.execute()
  }
}
