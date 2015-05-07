package kamon.zipkin.actor

import akka.actor._
import kamon.Kamon
import kamon.trace.{ HierarchyConfig, TraceInfo }
import kamon.zipkin.{ Zipkin, ZipkinConfig }
import kamon.zipkin.models._

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

class ZipkinActorSupervisor(spansSubmitter: ActorRef) extends Actor with ActorLogging {

  val tokenActors = TrieMap.empty[String, Option[ActorRef]]

  def receive: Receive = {
    case trace: TraceInfo ⇒
      trace.metadata.get(HierarchyConfig.rootToken) match {
        case Some(rootToken) ⇒
          tokenActors.get(rootToken) match {
            case None ⇒
              val tokenActor = context.actorOf(Props(new ZipkinActor(spansSubmitter, rootToken,
                trace.metadata.contains(ZipkinConfig.remote))))
              tokenActors += (rootToken -> Some(tokenActor))
              tokenActor ! trace
            case Some(Some(tokenActor)) ⇒ tokenActor ! trace
            case Some(None)             ⇒
          }
        case None ⇒ log.warning(s"TraceInfo doesn't have a ${HierarchyConfig.rootToken}")
      }
    case query: ActorRef ⇒ query ! tokenActors.filter(_._2.isDefined).flatMap(a ⇒ List(a._1))
    case rootToken: String ⇒
      tokenActors.get(rootToken) match {
        case Some(Some(zipkinActor)) ⇒
          tokenActors += (rootToken -> None)
          zipkinActor ! PoisonPill
        case Some(None) ⇒ log.error("Trying to end trace which already ended")
        case None       ⇒ log.error("Trying to end trace which doesn't exist")
      }
  }

}

class ZipkinActor(spansSubmitter: ActorRef, rootToken: String, remote: Boolean) extends Actor with ActorLogging {

  lazy val config = Kamon(Zipkin).config
  val traceSpan = TrieMap.empty[String, Span]

  def receive: Receive = {
    case trace: TraceInfo ⇒
      // Create span of this TraceInfo and add it to the TrieMap
      traceSpan += (trace.token -> traceInfoToSpans(trace))

      // Send spans to Zipkin
      if (trace.token == rootToken || (remote && trace.metadata.getOrElse(ZipkinConfig.spanClass, "") == ZipkinConfig.endpointMarker)) {
        spansSubmitter ! new SpanBlock(traceSpan)
        sender() ! rootToken
      }
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