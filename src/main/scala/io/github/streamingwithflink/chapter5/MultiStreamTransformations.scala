package io.github.streamingwithflink.chapter5

import java.time.Duration

import io.github.streamingwithflink.chapter5.util.{Alert, SmokeLevel, SmokeLevelSource}
import io.github.streamingwithflink.chapter5.util.SmokeLevel.SmokeLevel
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * A simple application that outputs an alert whenever there is a high risk of fire.
  * The application receives the stream of temperature sensor readings and a stream of smoke level measurements.
  * When the temperature is over a given threshold and the smoke level is high, we emit a fire alert.
  */
object MultiStreamTransformations {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val tempReadings: DataStream[SensorReading] = env
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

    // ingest smoke level stream
    val smokeReadings: DataStream[SmokeLevel] = env
      .addSource(new SmokeLevelSource)
      .setParallelism(1)

    // group sensor readings by their id
    val keyed: KeyedStream[SensorReading, String] = tempReadings
      .keyBy(_.id)

    // connect the two streams and raise an alert if the temperature and smoke levels are high
    // 连接两个流, 如果温度和冒烟等级都很高则发出告警
    val alerts = keyed
      .connect(smokeReadings.broadcast)
      .flatMap(new RaiseAlertFlatMap)

    alerts.print()

    // execute application
    env.execute("Multi-Stream Transformations Example")
  }

  /**
    * A CoFlatMapFunction that processes a stream of temperature readings ans a control stream
    * of smoke level events. The control stream updates a shared variable with the current smoke level.
    * For every event in the sensor stream, if the temperature reading is above 100 degrees
    * and the smoke level is high, a "Risk of fire" alert is generated.
    */
  class RaiseAlertFlatMap extends CoFlatMapFunction[SensorReading, SmokeLevel, Alert] {

    var smokeLevel: SmokeLevel.Value = SmokeLevel.Low

    override def flatMap1(in1: SensorReading, collector: Collector[Alert]): Unit = {
      // high chance of fire => true
      if (smokeLevel.equals(SmokeLevel.High) && in1.temperature > 100) {
        collector.collect(Alert("Risk of fire!", in1.timestamp))
      }
    }

    override def flatMap2(in2: SmokeLevel, collector: Collector[Alert]): Unit = {
      smokeLevel = in2
    }
  }
}
