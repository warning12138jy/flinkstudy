package State

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import study.{ClickSource, Event}

object chapter06 {
  def main(args: Array[String]): Unit = {
    // 获取流执行环境
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据源
    val eventStream = env.addSource(new ClickSource)

    //获取表环境
    val tableEnv = StreamTableEnvironment.create(env)
    //将数据流转换成表
    val envTable= tableEnv.fromDataStream(eventStream)
    //envTable.printSchema()
    //用执行 SQL 的方式提取数据
    val visitTable = tableEnv.sqlQuery("select url,user from "+envTable)
    // 将表转换成数据流，打印输出
    tableEnv.toDataStream(visitTable).print()

    env.execute("Table API And SQL")


  }

}
