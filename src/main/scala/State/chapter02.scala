package State
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object chapter02 {
  def main(args: Array[String]): Unit = {
    val env:StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream1 = env.fromElements(
      ("a", "stream-1", 1000L),
      ("b", "stream-1", 2000L)
    )
      .assignAscendingTimestamps(_._3)
    val stream2 = env.fromElements(
      ("a", "stream-2", 3000L),
      ("b", "stream-2", 4000L)
    )
      .assignAscendingTimestamps(_._3)

    stream1
      .keyBy(_._1)
      // 连接两条流
      .connect(stream2.keyBy(_._1))
      .process(new myCoProcessFunction)
      .print()

    env.execute("ListState")
  }

}
class myCoProcessFunction extends CoProcessFunction[(String,String,Long),(String,String,Long),String]{
    // 用来保存来自第一条流的事件的列表状态变量
    lazy val stream1ListState: ListState[(String,String,Long)] = getRuntimeContext.getListState(
      new ListStateDescriptor[(String,String,Long)]("stream1-list",classOf[(String,String,Long)])
    )
    // 用来保存来自第一条流的事件的列表状态变量
    lazy val stream2ListState: ListState[(String,String,Long)] = getRuntimeContext.getListState(
      new ListStateDescriptor[(String,String,Long)]("stream2-list",classOf[(String,String,Long)])
    )
  // 处理来自第一条流的事件
  override def processElement1(left: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit = {
    // 将事件添加到列表状态变量
    stream1ListState.add(left)
    // 导入隐式类型转换
    import scala.collection.JavaConversions._
    // 当前事件和第二条流的已经到达的事件做联结
    for (right <- stream2ListState.get){
      out.collect(left + "=>"+ right)
    }
  }
  // 处理来自第二条流的事件
  override def processElement2(right: (String, String, Long), ctx: CoProcessFunction[(String, String, Long), (String, String, Long), String]#Context, out: Collector[String]): Unit ={
    stream2ListState.add(right)
    import scala.collection.JavaConversions._
    // 当前事件和第一条流的已经到达的事件做联结
    for (left <- stream1ListState.get){
      out.collect(left + "=>" + right)
    }
  }
}