/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.streamingwithflink.chapter5.util

import io.github.streamingwithflink.chapter5.util.SmokeLevel.SmokeLevel
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
  * Flink SourceFunction to generate random SmokeLevel events.
  */
class SmokeLevelSource extends RichParallelSourceFunction[SmokeLevel] {

  // flag indicating whether source is still running.
  // 标记: 表明 source 是否仍在运行
  var running: Boolean = true

  /** run() continuously emits SmokeLevel events by emitting them through the SourceContext. */
  /* run() 通过 SourceContext 持续发射 SmokeLevel 事件*/
  override def run(srcCtx: SourceContext[SmokeLevel]): Unit = {

    // initialize random number generator
    // 初始化随机数生成器
    val rand = new Random()

    // emit data until being canceled
    // 发射数据直到被取消
    while (running) {
      if (rand.nextGaussian() > 0.8 ) {
        // emit a high SmokeLevel
        srcCtx.collect(SmokeLevel.High)
      }
      else {
        // otherwise emit a low SmokeLevel
        srcCtx.collect(SmokeLevel.Low)
      }

      // wait for 1s
      Thread.sleep(1000)
    }
  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }

}
