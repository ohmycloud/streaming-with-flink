package io.github.streamingwithflink.chapter5

import org.apache.flink.streaming.api.scala._

object RollingSum {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    // 滚动求和
    val resultStream: DataStream[(Int, Int)] = inputStream
      .keyBy(x => x._1) // key on first field of the tuple
      .sum(1)   // sum the second field of the tuple
      .map(x => (x._1, x._2))

    resultStream.print()

    // execute application
    env.execute("Rolling Sum Example")
  }

}
