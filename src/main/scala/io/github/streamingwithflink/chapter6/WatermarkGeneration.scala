package io.github.streamingwithflink.chapter6

import java.time.Duration

import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark

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
      //.assignTimestampsAndWatermarks(new PeriodicAssigner)

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
      //.assignTimestampsAndWatermarks(new PunctuatedAssigner)

    readingsWithPeriodicWMs.print()
//    readingsWithPunctuatedWMs.print()

    env.execute("Assign timestamps and generate watermarks")
  }
}

/**
  * Assigns timestamps to records and provides watermarks with a 1 minute out-of-ourder bound when being asked.
  */
class PeriodicAssigner extends AssignerWithPeriodicWatermarks[SensorReading] {

  // 1 min in ms
  val bound: Long = 60 * 1000
  // the maximum observed timestamp
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(r: SensorReading, previousTS: Long): Long = {
    // update maximum timestamp
    maxTs = maxTs.max(r.timestamp)
    // return record timestamp
    r.timestamp
  }
}

/**
  * Assigns timestamps to records and emits a watermark for each reading with sensorId == "sensor_1".
  */
class PunctuatedAssigner extends AssignerWithPunctuatedWatermarks[SensorReading] {

  // 1 min in ms
  val bound: Long = 60 * 1000

  override def checkAndGetNextWatermark(r: SensorReading, extractedTS: Long): Watermark = {
    if (r.id == "sensor_1") {
      // emit watermark if reading is from sensor_1
      new Watermark(extractedTS - bound)
    } else {
      // do not emit a watermark
      null
    }
  }

  override def extractTimestamp(r: SensorReading, previousTS: Long): Long = {
    // assign record timestamp
    r.timestamp
  }
}
