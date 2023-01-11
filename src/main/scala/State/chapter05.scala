package State

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import study.{ClickSource, Event}

import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
object chapter05 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStateBackend(new RocksDBStateBackend(""))
      .enableCheckpointing(TimeUnit.SECONDS.toMillis(100),CheckpointingMode.AT_LEAST_ONCE)
    env.addSource(new ClickSource)
      .assignAscendingTimestamps(_.timestamp)
      .addSink(new BufferingSink(10))
    env.execute("Operator State")
  }
  //实现 SinkFunction 和 CheckpointedFunction 这两个接口
  class BufferingSink(threshold: Int) extends SinkFunction[Event]
    with CheckpointedFunction {
    private var checkpointedState: ListState[Event] = _
    private val bufferedElements = ListBuffer[Event]()
    // 每来一条数据调用一次 invoke()方法
    override def invoke(value: Event, context: Context): Unit = {
      // 将数据先缓存起来
      bufferedElements += value
      // 当缓存中的数据量到达了阈值，执行 sink 逻辑
      if (bufferedElements.size == threshold) {
        for (element <- bufferedElements) {
          // 输出到外部系统，这里用控制台打印模拟
          println(element)
        }
        println("==========输出完毕=========")
        // 清空缓存
        bufferedElements.clear()
      }
    }
    // 对状态做快照
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      checkpointedState.clear() // 清空状态变量
      // 把当前局部变量中的所有元素写入到检查点中
      for (element <- bufferedElements) {
        // 将缓存中的数据写入状态变量
        checkpointedState.add(element)
      }
    }
    // 初始化算子状态变量
    override def initializeState(context: FunctionInitializationContext): Unit =
    {
      val descriptor = new ListStateDescriptor[Event](
        "buffered-elements",
        classOf[Event]
      )
      // 初始化状态变量
      checkpointedState = context.getOperatorStateStore.getListState(descriptor)
      // 如果是从故障中恢复，就将 ListState 中的所有元素添加到局部变量中
      if (context.isRestored) {
        import scala.collection.JavaConversions._
        for (element <- checkpointedState.get()) {
          bufferedElements += element
        }
      }
    }
  }
}
