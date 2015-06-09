package kamon.zipkin.actor

import akka.actor._
import kamon.Kamon
import kamon.trace.{ HierarchyConfig, TraceInfo }
import kamon.zipkin.{ Zipkin, ZipkinConfig }
import kamon.zipkin.models._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class ZipkinActorSupervisor(spansSubmitter: ActorRef) extends Actor with ActorLogging {

  val tokenActors = mutable.Map.empty[String, ActorRef]

  def receive: Actor.Receive = {
    case trace: TraceInfo ⇒
      trace.metadata.get(HierarchyConfig.rootToken) match {
        case Some(rootToken) ⇒
          tokenActors.get(rootToken) match {
            case None ⇒
              val tokenActor = context.actorOf(Props(new ZipkinActor(spansSubmitter, rootToken,
                trace.metadata.contains(ZipkinConfig.remote))))
              tokenActors.put(rootToken, tokenActor)
              tokenActor ! trace
            case Some(tokenActor) ⇒
              tokenActor ! trace
          }
        case None ⇒ log.warning(s"TraceInfo doesn't have the parameter ${HierarchyConfig.rootToken}")
      }
  }

}

class ZipkinActor(spansSubmitter: ActorRef, rootToken: String, remote: Boolean) extends Actor with ActorLogging {
  import context._

  lazy val config = Kamon(Zipkin).config
  val traceSpan = mutable.Map.empty[String, Span]

  override def preStart() =
    system.scheduler.scheduleOnce(config.scheduler, self, "end")

  def receive: Actor.Receive = {
    case trace: TraceInfo ⇒
      // Create span of this TraceInfo and add it to the TrieMap
      traceSpan.put(trace.token, traceInfoToSpans(trace))
      system.scheduler.scheduleOnce(config.scheduler, self, "end")

    // Send spans to Zipkin
    case "end" ⇒
      spansSubmitter ! new SpanBlock(traceSpan, rootToken, remote)
      context.become(emptyReceive)
  }

  def emptyReceive: Actor.Receive = {
    case trace: TraceInfo ⇒ log.warning("IGNORE : " + trace.metadata.getOrElse(ZipkinConfig.spanClass, "error"))
  }

  private def traceInfoToSpans(trace: TraceInfo): Span = {
    val token = trace.token
    val parentToken = trace.metadata.getOrElse(HierarchyConfig.parentToken, "")

    val cleanMetaData = mutable.Map(trace.metadata
      .filterKeys(k ⇒ !k.startsWith(ZipkinConfig.internalPrefix))
      .toSeq: _*)
    val endpoint = Endpoint.createEndpoint(trace.metadata.getOrElse(ZipkinConfig.spanClass, ZipkinConfig.rootName), config.service.host, config.service.port)

    Span(trace, rootToken, token, trace.name, TimestampConverter.timestampToMicros(trace.timestamp),
      TimestampConverter.durationToMicros(trace.elapsedTime), cleanMetaData, parentToken, endpoint)
  }
}