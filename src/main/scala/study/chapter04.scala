package study

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala._


object chapter04 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
//    val stream = env.addSource(new ClickSource)
//    stream.broadcast.print("broadcast").setParallelism(4)
    env.fromElements(1,2,3,4,5,6,7,8,9)
      //自定义分区
      .partitionCustom(
        new Partitioner[Int] {
          override def partition(key: Int, numPartitions: Int): Int = key % 2
        },
        data => data //// 以自身作为 key
      )
      .print()
    env.execute()
  }

}
