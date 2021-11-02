package io.github.streamingwithflink.chapter8

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}
import io.github.streamingwithflink.chapter8.util.{DerbySetup, DerbyWriter}
import io.github.streamingwithflink.util.{SensorReading, SensorSource}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

/**
  * 示例程序: 演示了使用 AsyncFunction 来充实存储在外部数据库中的数据记录。
 *  AsyncFunction 通过它的 JDBC 接口查询数据库。
 *  对于这个示例程序, 使用的是嵌入式的内存数据库 Derby。
 *  AsyncFunction 发送查询并在单独的线程中以异步的方式处理它们的结果, 以改善延迟和吞吐量。
 *  该程序包含了一个 MapFunction, 它以同步的方式执行和 AsyncFunction 同样的逻辑。
 *  你可以注释掉代码的一部分以比较同步的 MapFunction 和 AsyncFunction 的行为。
  */
object AsyncFunctionExample {

  def main(args: Array[String]): Unit = {

    // 设置嵌入式 Derby 数据库
    DerbySetup.setupDerby(
      """CREATE TABLE SensorLocations (
        |  sensor VARCHAR(16) PRIMARY KEY,
        |  room VARCHAR(16))
      """.stripMargin)

    // 插入一些初始数据
    DerbySetup.initializeTable(
      "INSERT INTO SensorLocations (sensor, room) VALUES (?, ?)",
      (1 to 80).map(i => Array(s"sensor_$i", s"room_${i % 10}")).toArray
        .asInstanceOf[Array[Array[Any]]]
    )

    // 启动一个线程用以更新 Derby 表中的数据
    new Thread(new DerbyWriter(
      "UPDATE SensorLocations SET room = ? WHERE sensor = ?",
      (rand: Random) =>
        Array(s"room_${1 + rand.nextInt(20)}", s"sensor_${1 + rand.nextInt(80)}"),
      500L
    )).start()

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val readings: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource).setParallelism(8)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(
        // Flink 1.11 use new interface of assignTimestampsAndWatermarks that accept a WatermarkStrategy
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })
      )

    // OPTION 1 (comment out to disable)
    // --------
    // 使用异步请求从 Derby 表中查询传感器的位置
    val sensorLocations: DataStream[(String, String)] = AsyncDataStream
      .orderedWait(
        readings,
        new DerbyAsyncFunction,
        5, TimeUnit.SECONDS,        // timeout requests after 5 seconds
        100)                        // at most 100 concurrent requests

    // OPTION 2 (uncomment to enable)
    // --------
    // 使用同步请求从 Derby 表中查询传感器的位置
//    val sensorLocations: DataStream[(String, String)] = readings
//      .map(new DerbySyncFunction)

    // print the sensor locations
    sensorLocations.print()

    env.execute()
  }
}

/**
  * AsyncFunction that queries a Derby table via JDBC in a non-blocking fashion.
  *
  * Since the JDBC interface does not support asynchronous queries, starts individual threads to
  * concurrently query Derby and handle the query results in an non-blocking fashion.
  */
class DerbyAsyncFunction extends AsyncFunction[SensorReading, (String, String)] {

  // caching execution context used to handle the query threads
  private lazy val cachingPoolExecCtx =
    ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
  // direct execution context to forward result future to callback object
  private lazy val directExecCtx =
    ExecutionContext.fromExecutor(
      org.apache.flink.util.concurrent.Executors.directExecutor())

  /** Executes JDBC query in a thread and handles the resulting Future
    * with an asynchronous callback. */
  override def asyncInvoke(
      reading: SensorReading,
      resultFuture: ResultFuture[(String, String)]): Unit = {

    val sensor = reading.id

    // get room from Derby table as Future
    val room: Future[String] = Future {
      // Creating a new connection and statement for each record.
      // Note: This is NOT best practice!
      // Connections and prepared statements should be cached.
      val conn = DriverManager
        .getConnection("jdbc:derby:memory:flinkExample", new Properties())
      val query = conn.createStatement()

      // submit query and wait for result. this is a synchronous call.
      val result = query.executeQuery(
        s"SELECT room FROM SensorLocations WHERE sensor = '$sensor'")

      // get room if there is one
      val room = if (result.next()) {
        result.getString(1)
      } else {
        "UNKNOWN ROOM"
      }

      // close resultset, statement, and connection
      result.close()
      query.close()
      conn.close()

      // sleep to simulate (very) slow requests
      Thread.sleep(2000L)

      // return room
      room
    }(cachingPoolExecCtx)

    // apply result handling callback on the room future
    room.onComplete {
      case Success(r) => resultFuture.complete(Seq((sensor, r)))
      case Failure(e) => resultFuture.completeExceptionally(e)
    }(directExecCtx)
  }
}

/**
  * MapFunction 查询, 通过 JDBC 以阻塞的形式查询 Derby 表。
  */
class DerbySyncFunction extends RichMapFunction[SensorReading, (String, String)] {

  var conn: Connection = _
  var query: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 连接到 Derby 并准备查询
    this.conn = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties())
    this.query = conn.prepareStatement("SELECT room FROM SensorLocations WHERE sensor = ?")
  }

  override def map(reading: SensorReading): (String, String) = {

    val sensor = reading.id

    // 设置查询参数并执行查询
    query.setString(1, sensor)
    val result = query.executeQuery()

    // get room if there is one
    val room = if (result.next()) {
      result.getString(1)
    } else {
      "UNKNOWN ROOM"
    }
    result.close()

    // sleep to simulate (very) slow requests
    Thread.sleep(2000L)

    // return sensor with looked up room
    (sensor, room)
  }

  override def close(): Unit = {
    super.close()
    this.query.close()
    this.conn.close()
  }
}
