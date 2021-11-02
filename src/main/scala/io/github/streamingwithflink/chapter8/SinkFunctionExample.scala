package io.github.streamingwithflink.chapter8

import java.io.PrintStream
import java.net.{InetAddress, Socket}
import java.time.Duration
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

/**
  * 把传感器读数写到 socket 中
  *
  * 注意: 在启动该程序之前, 你需要启动一个进程来监听 localhost:9191 上的 socket
  * 在 Linux 上, 你可以使用下面的 nc (netcat) 命令:
  *
  * nc -l localhost 9191
 *
 * 在 Windows 上, 使用 choco install netcat 命令安装 nc, 再使用如下命令监听:
 *
 * nc -l localhost -p 9191
  */
object SinkFunctionExample {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(
        // Flink 1.11 use new interface of assignTimestampsAndWatermarks that accept a WatermarkStrategy
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })
      )

    // write the sensor readings to a socket
    readings.addSink(new SimpleSocketSink("localhost", 9191))
      // set parallelism to 1 because only one thread can write to a socket
      .setParallelism(1)

    env.execute()
  }
}

/**
  * Writes a stream of [[SensorReading]] to a socket.
  */
class SimpleSocketSink(val host: String, val port: Int)
    extends RichSinkFunction[SensorReading] {

  var socket: Socket = _
  var writer: PrintStream = _

  override def open(config: Configuration): Unit = {
    // open socket and writer
    socket = new Socket(InetAddress.getByName(host), port)
    writer = new PrintStream(socket.getOutputStream)
  }

  override def invoke(
      value: SensorReading,
      ctx: SinkFunction.Context): Unit = {
    // write sensor reading to socket
    writer.println(value.toString)
    writer.flush()
  }

  override def close(): Unit = {
    // close writer and socket
    writer.close()
    socket.close()
  }
}
