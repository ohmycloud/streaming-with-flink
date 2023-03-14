package io.github.streamingwithflink.chapter5

import java.time.Duration
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._

/** Object that defines the DataStream program in the main() method */
object KeyedTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })
      )


    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = readings
      .keyBy(_.id)

//    keyed.print()

    // a rolling reduce that computes the highest temperature of each sensor and
    // the corresponding timestamp
    // 不断计算出每个传感器当前的最高温度
    val maxTempPerSensor: DataStream[SensorReading] = keyed
      .reduce((r1, r2) => {
        if (r1.temperature > r2.temperature) r1 else r2
      })

    maxTempPerSensor.print()

//    maxTempPerSensor.filter(x => x.id == "sensor_107").print()

    // execute application
    env.execute("Keyed Transformations Example")
  }

}
