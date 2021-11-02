package io.github.streamingwithflink.chapter8

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.Duration
import java.util.Properties
import io.github.streamingwithflink.chapter8.util.{DerbyReader, DerbySetup}
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * 样例程序: 使用 UPSERT 写发射传感器读数到嵌入的内存式 Apache Derby 数据库中
  *
  * 一个单独的线程每隔 10 秒查询一次数据库, 然后打印结果
  */
object IdempotentSinkFunctionExample {

  def main(args: Array[String]): Unit = {

    // 设置嵌入式 Derby 数据库
    DerbySetup.setupDerby(
      """
        |CREATE TABLE Temperatures (
        |  sensor VARCHAR(16) PRIMARY KEY,
        |  temp DOUBLE)
      """.stripMargin)

    // 启动一个线程每个 10 秒钟打印一次写入到 Derby 数据库的数据
    new Thread(
        new DerbyReader("SELECT sensor, temp FROM Temperatures ORDER BY sensor", 10000L))
      .start()

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

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


    val celsiusReadings: DataStream[SensorReading] = sensorData
      // 使用一个内联的 map 函数把华氏温度转换为摄氏温度
      .map( r => SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)) )

    // 把转换后的传感器读数写入到 Derby 数据库
    celsiusReadings.addSink(new DerbyUpsertSink)

    env.execute()
  }
}

/**
  * Sink that upserts SensorReadings into a Derby table that is keyed on the sensor id.
  *
  * Since Derby does not feature a dedicated UPSERT command, we execute an UPDATE statement first
  * and execute an INSERT statement if UPDATE did not modify any row.
  */
class DerbyUpsertSink extends RichSinkFunction[SensorReading] {

  var conn: Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    // 连接到嵌入式内存数据库 Derby
    val props = new Properties()
    conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", props)
    // 更新插入和更新语句
    insertStmt = conn.prepareStatement(
      "INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)")
    updateStmt = conn.prepareStatement(
      "UPDATE Temperatures SET temp = ? WHERE sensor = ?")
  }

  /**
   *
   * @param r SensorReading
   * @param context Context
   */
  override def invoke(r: SensorReading, context: Context): Unit = {
    // 为更新语句设置参数, 然后执行更新语句
    updateStmt.setDouble(1, r.temperature)
    updateStmt.setString(2, r.id)
    updateStmt.execute()

    // 如果更新语句没有更新任何行, 则执行插入语句
    if (updateStmt.getUpdateCount == 0) {
      // 为插入语句设置参数
      insertStmt.setString(1, r.id)
      insertStmt.setDouble(2, r.temperature)

      // 执行插入语句
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
