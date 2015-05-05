package kamon.zipkin.actor

import akka.actor._
import kamon.Kamon
import kamon.trace.{ HierarchyConfig, TraceInfo }
import kamon.util.NanoTimestamp
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
  // Associate to each token corresponding span
  val traceSpan = TrieMap.empty[String, Span]
  // Associate to each instance corresponding token
  val traceInstance = TrieMap.empty[String, String]
  // Associate to each token corresponding real parentToken
  val traceParent = TrieMap.empty[String, String]

  def receive: Receive = {
    case trace: TraceInfo ⇒
      // Create span of this TraceInfo and add it to the TrieMap
      traceSpan += (trace.token -> traceInfoToSpans(trace))

      checkUnicity(trace)

      // Send spans to Zipkin
      if (trace.token == rootToken || (remote && trace.metadata.getOrElse(ZipkinConfig.spanClass, "") == ZipkinConfig.endpointMarker)) {
        sender() ! rootToken
        spansSubmitter ! new SpanBlock(traceSpan.map(_._2.simpleSpan))
      }
  }

  override def postStop() = {
    traceSpan.clear()
    traceInstance.clear()
    traceParent.clear()
  }

  // Check if this instance is part of an already existing span
  private def checkUnicity(trace: TraceInfo) = {
    val instance = trace.metadata.getOrElse(ZipkinConfig.spanUniqueClass, "")

    traceInstance.get(instance) match {
      case Some(realParentToken) ⇒ traceSpan.get(realParentToken) match {
        case Some(span) ⇒
          span.addToRecords(
            ZipkinConfig.segmentBegin + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
            new NanoTimestamp(trace.timestamp.nanos))
          span.addToRecords(
            ZipkinConfig.segmentEnd + trace.metadata.getOrElse(ZipkinConfig.spanType, "unknown"),
            new NanoTimestamp(trace.timestamp.nanos + trace.elapsedTime.nanos))
          span.addSegments(trace.segments)

          traceSpan -= trace.token
          traceParent += (trace.token -> realParentToken)
        case None ⇒
          traceInstance += (instance -> trace.token)
      }
      case None ⇒ traceInstance += (instance -> trace.token)
    }
  }

  private def traceInfoToSpans(trace: TraceInfo): Span = {
    val parentMeta = trace.metadata.getOrElse(HierarchyConfig.parentToken, "")
    val parentToken = traceParent.getOrElse(parentMeta, parentMeta)
    val token = trace.token

    val traceId = longHash(rootToken)
    val rootSpanId = longHash(token)
    val parentSpanId = longHash(parentToken)

    val cleanMetaData = mutable.Map(trace.metadata
      .filterKeys(k ⇒ !k.startsWith(ZipkinConfig.internalPrefix) && k != HierarchyConfig.rootToken && k != HierarchyConfig.parentToken).toSeq: _*)
    val endpoint = Endpoint.createEndpoint(trace.metadata.getOrElse(ZipkinConfig.spanClass, "Request"), config.service.host, config.service.port)

    Span(trace, traceId, rootSpanId, trace.name, TimestampConverter.timestampToMicros(trace.timestamp),
      TimestampConverter.durationToMicros(trace.elapsedTime), cleanMetaData, parentSpanId, endpoint)
  }

  private def longHash(string: String): Long = {
    var h = 1125899906842597L
    val len = string.length
    for (i ← 0 until len) h = 31 * h + string.charAt(i)
    h
  }
}