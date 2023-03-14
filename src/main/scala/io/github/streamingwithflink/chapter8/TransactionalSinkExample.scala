package io.github.streamingwithflink.chapter8

import java.io.BufferedWriter
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime, ZoneId, ZoneOffset}
import io.github.streamingwithflink.chapter8.util.FailingMapper
import io.github.streamingwithflink.util.{ResettableSensorSource, SensorReading}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 示例程序: 演示了两步提交(2PC) sink 行为, 这个 sink 将输出写入到文件中。
 *
 * 2PC sink 通过立即将记录写入临时文件来保证精确的一次输出。对于每个检查点，都会创建一个新的临时文件。当一个检查点完成后，相应的临时文件被移动到一个目标目录中，从而提交。
 * 该程序包括一个 MapFunction，它以一定的间隔抛出一个异常，以模拟应用程序的失败。你可以比较 2PC sink 和普通 print sink 在故障情况下的行为。
 * TransactionalFileSink 在检查点完成后将文件提交到目标目录，并防止重复的结果输出。
 * 普通的 print() sink 在产生结果时写到标准输出，并在失败的情况下重复输出结果。
 */
object TransactionSinkExample {

  def main(args: Array[String]): Unit = {

    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest sensor stream
    val sensorData: DataStream[SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new ResettableSensorSource)
      // assign timestamps and watermarks which are required for event time
      .assignTimestampsAndWatermarks(
        // Flink 1.11 use new interface of assignTimestampsAndWatermarks that accept a WatermarkStrategy
        WatermarkStrategy
          .forBoundedOutOfOrderness(Duration.ofSeconds(5))
          .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading] {
            override def extractTimestamp(t: SensorReading, l: Long): Long = t.timestamp
          })
      )

    // compute average temperature of all sensors every second
    val avgTemp: DataStream[(String, Double)] = sensorData
      .timeWindowAll(Time.seconds(1))
      .apply((w, vals, out: Collector[(String, Double)]) => {
          val avgTemp = vals.map(_.temperature).sum / vals.count(_ => true)
          // format window timestamp as ISO timestamp string
          val epochSeconds = w.getEnd / 1000
          val tString = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          // emit record
          out.collect((tString, avgTemp))
        }
      )
      // generate failures to trigger job recovery
      .map(new FailingMapper[(String, Double)](16)).setParallelism(1)

    // OPTION 1 (comment out to disable)
    // --------
    // write to files with a transactional sink.
    // results are committed when a checkpoint is completed.
    val (targetDir, transactionDir) = createAndGetPaths
    avgTemp.addSink(new TransactionalFileSink(targetDir, transactionDir))

    // OPTION 2 (uncomment to enable)
    // --------
    // print to standard out without write-ahead log.
    // results are printed as they are produced and re-emitted in case of a failure.
//    avgTemp.print()
//      // enforce sequential writing
//      .setParallelism(1)

    env.execute()
  }

  /** Creates temporary paths for the output of the transactional file sink. */
  def createAndGetPaths: (String, String) = {
    val tempDir = System.getProperty("java.io.tmpdir")
    val targetDir = s"$tempDir/committed"
    val transactionDir = s"$tempDir/transaction"

    val targetPath = Paths.get(targetDir)
    val transactionPath = Paths.get(transactionDir)

    if (!Files.exists(targetPath)) {
      Files.createDirectory(targetPath)
    }
    if (!Files.exists(transactionPath)) {
      Files.createDirectory(transactionPath)
    }

    (targetDir, transactionDir)
  }
}

/**
  * Transactional sink that writes records to files an commits them to a target directory.
  *
  * Records are written as they are received into a temporary file. For each checkpoint, there is
  * a dedicated file that is committed once the checkpoint (or a later checkpoint) completes.
  */
class TransactionalFileSink(val targetPath: String, val tempPath: String)
    extends TwoPhaseCommitSinkFunction[(String, Double), String, Void](
      createTypeInformation[String].createSerializer(new ExecutionConfig),
      createTypeInformation[Void].createSerializer(new ExecutionConfig)) {

  var transactionWriter: BufferedWriter = _

  /**
    * Creates a temporary file for a transaction into which the records are
    * written.
    */
  override def beginTransaction(): String = {

    // path of transaction file is constructed from current time and task index
    val timeNow = LocalDateTime.now(ZoneId.of("UTC"))
      .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask
    val transactionFile = s"$timeNow-$taskIdx"

    // create transaction file and writer
    val tFilePath = Paths.get(s"$tempPath/$transactionFile")
    Files.createFile(tFilePath)
    this.transactionWriter = Files.newBufferedWriter(tFilePath)
    println(s"Creating Transaction File: $tFilePath")

    // name of transaction file is returned to later identify the transaction
    transactionFile
  }

  /** Write record into the current transaction file. */
  override def invoke(transaction: String, value: (String, Double), context: Context): Unit = {
    transactionWriter.write(value.toString)
    transactionWriter.write('\n')
  }

  /** Flush and close the current transaction file. */
  override def preCommit(transaction: String): Unit = {
    transactionWriter.flush()
    transactionWriter.close()
  }

  /** Commit a transaction by moving the pre-committed transaction file
    * to the target directory.
    */
  override def commit(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    // check if the file exists to ensure that the commit is idempotent.
    if (Files.exists(tFilePath)) {
      val cFilePath = Paths.get(s"$targetPath/$transaction")
      Files.move(tFilePath, cFilePath)
    }
  }

  /** Aborts a transaction by deleting the transaction file. */
  override def abort(transaction: String): Unit = {
    val tFilePath = Paths.get(s"$tempPath/$transaction")
    if (Files.exists(tFilePath)) {
      Files.delete(tFilePath)
    }
  }
}