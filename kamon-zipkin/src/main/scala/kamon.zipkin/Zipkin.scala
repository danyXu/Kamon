/* =========================================================================================
 * Copyright © 2013-2015 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not use this file
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

package kamon.zipkin

import akka.actor._
import akka.event.Logging
import com.github.levkhomich.akka.tracing.TracingExtension
import kamon.Kamon
import kamon.trace.TraceSettings
import kamon.zipkin.actor.{ ZipkinActorSupervisor, SpanSubmitter }

object Zipkin extends ExtensionId[ZipkinExtension] with ExtensionIdProvider {
  override def lookup(): ExtensionId[_ <: Extension] = Zipkin
  override def createExtension(system: ExtendedActorSystem): ZipkinExtension = new ZipkinExtension(system)
}

class ZipkinExtension(system: ExtendedActorSystem) extends Kamon.Extension {
  val log = Logging(system, classOf[ZipkinExtension])
  log.info(s"Starting the Kamon(Zipkin) extension")

  val trace = TracingExtension(system)
  val config = new ZipkinConfig(system.settings.config)

  val spansSubmitter = system.actorOf(Props(new SpanSubmitter(TraceSettings(system.settings.config), Some(trace))))
  val zipkinActor = system.actorOf(Props(new ZipkinActorSupervisor(spansSubmitter)))
  Kamon.tracer.subscribe(zipkinActor)
}