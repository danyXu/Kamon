package kamon.zipkin.instrumentation

import akka.actor.Actor
import kamon.Kamon
import kamon.trace.{ LevelOfDetail, HierarchyConfig, Tracer }
import kamon.zipkin.ZipkinConfig
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Around, Pointcut, Aspect }

@Aspect
class BaseZipkinInstrumentation {

  @Pointcut(value = "execution(* akka.actor.Actor.aroundReceive(..)) && (!within(kamon..*) || within(kamon.zipkin.instrumentation..*)) && args(receive,msg)", argNames = "receive,msg")
  def aroundReceivePointcut(receive: Actor.Receive, msg: Any) {}

  @Around(value = "aroundReceivePointcut(receive,msg)", argNames = "pjp,receive,msg")
  def receiveTracingAround(pjp: ProceedingJoinPoint, receive: Actor.Receive, msg: Any): Unit =
    if (!Tracer.currentContext.isEmpty && Tracer.currentContext.levelOfDetail != LevelOfDetail.MetricsOnly) {
      val (rootToken: String, parentToken: String, parentClass: String, remote: Boolean) = Tracer.currentContext.token.split(HierarchyConfig.tokenSeparator) match {
        case Array(root, parentClassInstance, parent) ⇒ (root, parent, parentClassInstance, true)
        case Array(root, parent)                      ⇒ (root, parent, ZipkinConfig.rootName, true)
        case Array(parent) ⇒ Tracer.currentContext.metadata.get(HierarchyConfig.rootToken) match {
          case Some(token) ⇒ (token, parent, Tracer.currentContext.metadata.get(HierarchyConfig.spanUniqueClass).get, false)
          case None ⇒
            Tracer.currentContext.addMetadata(HierarchyConfig.rootToken, parent)
            (parent, parent, ZipkinConfig.rootName, false)
        }
      }

      Tracer.withContext(Kamon.tracer.newContext(msg.getClass.getSimpleName)) {
        val child = Tracer.currentContext
        child.startTimestamp

        child.addMetadata(HierarchyConfig.rootToken, rootToken)
        child.addMetadata(HierarchyConfig.parentToken, parentToken)
        child.addMetadata(HierarchyConfig.spanUniqueClass, pjp.getTarget.toString)
        if (remote) child.addMetadata(ZipkinConfig.remote, "remote")
        child.addMetadata(ZipkinConfig.parentClass, parentClass)
        child.addMetadata(ZipkinConfig.spanClass, pjp.getTarget.getClass.getSimpleName)
        child.addMetadata(ZipkinConfig.spanType, msg.getClass.getSimpleName)

        // child.addMetadata(msg.getClass.getSimpleName, msg.toString)

        // child.startSegment(msg.getClass.getSimpleName, ZipkinConfig.internalPrefix + "type", "ZipkinConfig").finish()
        pjp.proceed()

        child.finish()
      }
    } else pjp.proceed()

  /*
  @Around("execution (* *(..)) && !within(kamon..*) && !within(akka..*) && !within(scala..*) && within(tv.teads..*)")
  def around(pjp: ProceedingJoinPoint): Any = {
    if (Kamon(Zipkin).config.getBoolean("trace-methods") && !Tracer.currentContext.isEmpty && Tracer.currentContext.metadata.get(RemoteConfig.rootToken).nonEmpty) {
      val ctx = Tracer.currentContext
      val segment = ctx.startSegment(pjp.getSignature.getName, "", "")
      val r = pjp.proceed()
      segment.finish()
      r
    } else {
      pjp.proceed()
    }
  }
  */

}