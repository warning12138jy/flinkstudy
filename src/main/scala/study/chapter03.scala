package study

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object chapter03 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromElements(
      Event("Mary", "./home", 1000L),
      Event("Bob", "./cart", 2000L),
      Event("Alice", "./prod?id=1", 5 * 1000L),
      Event("Cary", "./home", 60 * 1000L)
    )
      .map(new myMapFunction)
      .print()
    env.execute()
  }

}
class myMapFunction extends RichMapFunction[Event,Long](){
  override def open(parameters: Configuration): Unit = {
    println("索引为"+getRuntimeContext.getIndexOfThisSubtask+"的任务的开始")
  }

  override def map(value: Event): Long = {
    value.timestamp
  }

  override def close(): Unit = {
    println("索引为 " + getRuntimeContext.getIndexOfThisSubtask + " 的任务结束")
  }
}