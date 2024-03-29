package io.github.streamingwithflink.chapter5

import java.time.Duration
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/** Object that defines the DataStream program in the main() method */
object BasicTransformations {

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

    // filter out sensor measurements from sensors with temperature under 25 degrees
    val filteredSensors: DataStream[SensorReading] = readings
      .filter( r =>  r.temperature >= 25)
//    filteredSensors.print()

   // the following filter transformation using a UDF
    val filteredSensorsWithUdf: DataStream[SensorReading] = readings
      .filter(new TemperatureFilter(25))
//    filteredSensorsWithUdf.print()

    // project the id of each sensor reading
    val sensorIds: DataStream[String] = filteredSensors
      .map( r => r.id )
//    sensorIds.print()

    // the following map transformation using a UDF
     val sensorIdsWithUdf: DataStream[String] = readings
       .map(new ProjectionMap)
//    sensorIdsWithUdf.print()

    // split the String id of each sensor to the prefix "sensor" and sensor number
    val splitIds: DataStream[String] = sensorIds
      .flatMap( id => id.split("_") )
//    splitIds.print()

    // the following flatMap transformation using a UDF
     val splitIdsWithUdf: DataStream[String] = sensorIds
      .flatMap( new SplitIdFlatMap )
    splitIdsWithUdf.print()

    // print result stream to standard out
    // splitIds.print()

    // execute application
    env.execute("Basic Transformations Example")
  }

  /** User-defined FilterFunction to filter out SensorReading with temperature below the threshold */
  class TemperatureFilter(threshold: Long) extends FilterFunction[SensorReading] {
    override def filter(r: SensorReading): Boolean = r.temperature >= threshold
  }

  /** User-defined MapFunction to project a sensor's id */
  class ProjectionMap extends MapFunction[SensorReading, String] {
    override def map(r: SensorReading): String  = r.id
  }

  /** User-defined FlatMapFunction that splits a sensor's id String into a prefix and a number */
  class SplitIdFlatMap extends FlatMapFunction[String, String] {
    override def flatMap(id: String, collector: Collector[String]): Unit =
      id.split("_").foreach(collector.collect)
  }
}
