package study

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import java.sql.Timestamp

object chapter13upgrade {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)

    //按照url分组，求出每个url的访问量
    val urlStreamCount = stream.keyBy(_.url)
      //开窗口
      .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
    // 增量聚合函数和全窗口聚合函数结合使用
    // 计算结果是每个窗口中每个 url 的浏览次数
      .aggregate(new UrlViewCountAgg,new UrlViewCountResult)
    // 对结果中同一个窗口的统计数据，进行排序处理
    val result = urlStreamCount
      .keyBy(_.windowEnd)
      .process(new TopN(2))
    result.print()
    env.execute("TopNUpgrade")
  }
}


class TopN(n: Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{
  var urlViewCountListState:ListState[UrlViewCount] = _
  override def open(parameters: Configuration): Unit = {
    urlViewCountListState = getRuntimeContext.getListState(
      new ListStateDescriptor[UrlViewCount]("list-state",classOf[UrlViewCount])
    )
  }

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据就添加到列表状态变量中
    urlViewCountListState.add(value)
    // 注册一个定时器，由于来的数据的 windowEnd 是相同的，所以只会注册一个定时器
    ctx.timerService.registerEventTimeTimer(value.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 导入隐式类型转换
    import scala.collection.JavaConversions._
    // 下面的代码将列表状态变量里的元素取出，然后放入 List 中，方便排序
    val urlViewCountList = urlViewCountListState.get().toList
    // 由于数据已经放入 List 中，所以可以将状态变量手动清空了
    urlViewCountListState.clear()
    // 按照浏览次数降序排列
    urlViewCountList.sortBy(_.count)
    // 拼接要输出的字符串
    val result = new StringBuilder
    result.append("===============================\n")
    for (i <- 0 until n){
      val urlViewCount = urlViewCountList(i)
      result
        .append("浏览量 No." + (i + 1) + " ")
        .append("url: " + urlViewCount.url + " ")
        .append("浏览量：" + urlViewCount.count + " ")
        .append("窗口结束时间：" + new Timestamp(timestamp - 1L) + "\n")
    }
    result.append("=========================\n")
    out.collect(result.toString())
  }
}
