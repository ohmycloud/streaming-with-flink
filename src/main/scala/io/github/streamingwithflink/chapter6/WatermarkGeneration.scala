package io.github.streamingwithflink.chapter6

import java.time.Duration
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object WatermarkGeneration {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure interval of periodic watermark generation
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)

    val readingsWithPeriodicWMs = readings
      // assign timestamps and periodic watermarks
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })
      )

    val readingsWithPunctuatedWMs = readings
      // assign timestamps and punctuated watermarks
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withIdleness(Duration.ofMinutes(1))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })
      )

    readingsWithPeriodicWMs.print()
//    readingsWithPunctuatedWMs.print()

    env.execute("Assign timestamps and generate watermarks")
  }
}
