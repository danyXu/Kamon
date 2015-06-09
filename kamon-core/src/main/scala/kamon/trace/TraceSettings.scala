/*
 * =========================================================================================
 * Copyright © 2013-2015 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.trace

import kamon.util.ConfigTools.Syntax
import com.typesafe.config.Config
import kamon.util.NanoInterval

import scala.collection.concurrent.TrieMap

case class TraceSettings(levelOfDetail: LevelOfDetail, sampler: String ⇒ Sampler, token: String)

object TraceSettings {

  val samplers = TrieMap.empty[String, Sampler]

  def apply(config: Config): TraceSettings = {
    val tracerConfig = config.getConfig("kamon.trace")

    val detailLevel: LevelOfDetail = tracerConfig.getString("level-of-detail") match {
      case "metrics-only" ⇒ LevelOfDetail.MetricsOnly
      case "simple-trace" ⇒ LevelOfDetail.SimpleTrace
      case other          ⇒ sys.error(s"Unknown tracer level of detail [$other] present in the configuration file.")
    }

    def sampler: Sampler = {
      val tracer = tracerConfig.getString("sampling") match {
        case "random"    ⇒ new RandomSampler(tracerConfig.getInt("random-sampler.chance"))
        case "ordered"   ⇒ new OrderedSampler(tracerConfig.getInt("ordered-sampler.sample-interval"))
        case "threshold" ⇒ new ThresholdSampler(new NanoInterval(tracerConfig.getFiniteDuration("threshold-sampler.minimum-elapsed-time").toNanos))
        case "clock"     ⇒ new ClockSampler(new NanoInterval(tracerConfig.getFiniteDuration("clock-sampler.pause").toNanos))
        case _           ⇒ SampleAll
      }
      if (tracerConfig.getBoolean("combine-threshold"))
        new CombineSamplers(tracer, new ThresholdSampler(new NanoInterval(tracerConfig.getFiniteDuration("threshold-sampler.minimum-elapsed-time").toNanos)))
      else
        tracer
    }

    val uniqueSampler = sampler

    def getSampler(traceName: String): Sampler = {
      if (detailLevel == LevelOfDetail.MetricsOnly) NoSampling
      else {
        if (tracerConfig.getBoolean("sampling-by-name")) {
          samplers.get(traceName) match {
            case Some(s) ⇒ s
            case None ⇒
              val s = sampler
              samplers.put(traceName, s)
              s
          }
        } else
          uniqueSampler
      }
    }

    val token: String = tracerConfig.getString("token-name")

    TraceSettings(detailLevel, getSampler, token)
  }
}