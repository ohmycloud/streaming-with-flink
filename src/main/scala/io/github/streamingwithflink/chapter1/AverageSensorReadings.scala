package io.github.streamingwithflink.chapter1

import java.time.Duration
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object AverageSensorReadings {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time for the application
    // Flink 1.12 中已废弃 setStreamTimeCharacteristic 方法
    // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
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

    val avgTemp: DataStream[SensorReading] = sensorData
      // convert Fahrenheit to Celsius using an inlined map function
      .map( r =>
      SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )
      // organize stream by sensorId
      .keyBy(_.id)
      // group readings in 1 second windows
      .window(TumblingEventTimeWindows.of(Time.seconds(1)))
      // compute average temperature using a user-defined function
      .apply(new TemperatureAverager)

    // print result stream to standard out
    avgTemp.print()

    // execute application
    env.execute("Compute average sensor temperature")
  }
}

/** User-defined WindowFunction to compute the average temperature of SensorReadings */
class TemperatureAverager extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

  /** apply() is invoked once for each window */
  override def apply(
    sensorId: String,
    window: TimeWindow,
    vals: Iterable[SensorReading],
    out: Collector[SensorReading]): Unit = {

    // compute the average temperature
    val (cnt, sum) = vals.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
    val avgTemp = sum / cnt

    // emit a SensorReading with the average temperature
    out.collect(SensorReading(sensorId, window.getEnd, avgTemp))
  }
}
