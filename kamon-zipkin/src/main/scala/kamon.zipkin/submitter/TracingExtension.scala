/**
 * Copyright 2014 the Akka Tracing contributors. See AUTHORS for more details.
 * https://github.com/levkhomich/akka-tracing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kamon.zipkin.submitter

import java.io.{ PrintWriter, StringWriter }
import java.util.concurrent.atomic.AtomicBoolean
import kamon.zipkin.thrift.Span
import akka.actor._
import akka.stream.actor.{ ActorSubscriber, ActorPublisher }
import org.apache.thrift.transport.{ TSocket, TFramedTransport }

/**
 * Tracer instance providing trace related methods.
 * @param system parent actor system
 */
class TracingExtensionImpl(system: ActorSystem) extends Extension {

  import TracingExtension._

  private[this] val enabled = new AtomicBoolean(system.settings.config.getBoolean(AkkaTracingEnabled))

  private[submitter] val holder = {
    val config = system.settings.config

    if (config.hasPath(AkkaTracingHost) && isEnabled) {
      val transport = new TFramedTransport(
        new TSocket(config.getString(AkkaTracingHost), config.getInt(AkkaTracingPort)))
      system.actorOf(Props({
        val holder = new SpanHolder()
        val submitter = holder.context.actorOf(Props(classOf[SpanSubmitter], transport), "spanSubmitter")
        ActorPublisher(holder.self).subscribe(ActorSubscriber(submitter))
        holder
      }), "spanHolder")
    } else {
      system.actorOf(Props.empty)
    }
  }

  private[submitter] def isEnabled: Boolean =
    enabled.get()

  private[submitter] def markCollectorAsUnavailable(): Unit =
    enabled.set(false)

  private[submitter] def markCollectorAsAvailable(): Unit =
    if (!enabled.get()) enabled.set(system.settings.config.getBoolean(AkkaTracingEnabled))

  def submitSpans(spans: TraversableOnce[Span]): Unit = {
    if (isEnabled)
      holder ! SubmitSpans(spans)
  }

}

/**
 * Tracing extension.
 *
 * Configuration parameters:
 * - akka.tracing.host - Scribe or Zipkin collector host
 * - akka.tracing.port - Scribe or Zipkin collector port (9410 by default)
 * - akka.tracing.sample-rate - trace sample rate, means that every nth message will be sampled
 * - akka.tracing.enabled - defaults to true, can be used to disable tracing
 *
 */
object TracingExtension extends ExtensionId[TracingExtensionImpl] with ExtensionIdProvider {

  private[submitter] val AkkaTracingHost = "akka.tracing.host"
  private[submitter] val AkkaTracingPort = "akka.tracing.port"
  private[submitter] val AkkaTracingEnabled = "akka.tracing.enabled"

  override def lookup(): this.type =
    TracingExtension

  override def createExtension(system: ExtendedActorSystem): TracingExtensionImpl =
    new TracingExtensionImpl(system)

  override def get(system: ActorSystem): TracingExtensionImpl =
    super.get(system)

  private[submitter] def getStackTrace(e: Throwable): String = {
    val sw = new StringWriter
    e.printStackTrace(new PrintWriter(sw))
    e.getClass.getCanonicalName + ": " + sw.toString
  }

}
