package kamon.zipkin.actor

import akka.actor.Actor
import com.github.levkhomich.akka.tracing.TracingExtensionImpl

/*
 * This actor is used to submit spans to akka-tracing @ https://github.com/levkhomich/akka-tracing
 * It could be "improved" to directly submit spans to Zipkin without having to use a library
 */
class SpanSubmitter(tracingExt: TracingExtensionImpl) extends Actor {

  def receive = {
    case spanBlock: SpanBlock ⇒
      tracingExt.submitSpans(spanBlock.spans)
  }
}