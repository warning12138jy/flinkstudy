package study

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

import java.time.Duration

object chapter06 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.addSource(new ClickSource)
    //插入水位线逻辑
    .assignTimestampsAndWatermarks(
      //针对乱序流插入水位线，延迟时间设置为5s
      WatermarkStrategy.forBoundedOutOfOrderness[Event](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[Event] {
          override def extractTimestamp(element: Event, recordTimestamp: Long): Long = {
            element.timestamp
          }
        }
        )
    )
      .print()
    env.execute("watermarks")
  }

}
