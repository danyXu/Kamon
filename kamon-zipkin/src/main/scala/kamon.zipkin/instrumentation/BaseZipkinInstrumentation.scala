package kamon.zipkin.instrumentation

import akka.actor.Actor
import kamon.Kamon
import kamon.trace.{ HierarchyConfig, Tracer }
import kamon.zipkin.ZipkinConfig
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Around, Pointcut, Aspect }

@Aspect
class BaseZipkinInstrumentation {

  @Pointcut(value = "execution(* akka.actor.Actor.aroundReceive(..)) && (!within(kamon..*) || within(kamon.zipkin.instrumentation..*)) && args(receive,msg)", argNames = "receive,msg")
  def aroundReceivePointcut(receive: Actor.Receive, msg: Any) {}

  @Around(value = "aroundReceivePointcut(receive,msg)", argNames = "pjp,receive,msg")
  def receiveTracingAround(pjp: ProceedingJoinPoint, receive: Actor.Receive, msg: Any): Unit = {
    if (!Tracer.currentContext.isEmpty) {
      val tokenEncoded = Tracer.currentContext.token.split(HierarchyConfig.tokenSeparator)
      val parentToken = tokenEncoded.last
      var rootToken = Tracer.currentContext.metadata.getOrElse(HierarchyConfig.rootToken, parentToken)
      if (tokenEncoded.length > 1) rootToken = tokenEncoded.head
      if (rootToken == parentToken) Tracer.currentContext.addMetadata(HierarchyConfig.rootToken, rootToken)

      Tracer.withContext(Kamon.tracer.newContext(msg.getClass.getSimpleName)) {
        val child = Tracer.currentContext
        child.startTimestamp

        child.addMetadata(HierarchyConfig.rootToken, rootToken)
        child.addMetadata(HierarchyConfig.parentToken, parentToken)
        child.addMetadata(ZipkinConfig.spanClass, pjp.getTarget.getClass.getSimpleName)
        child.addMetadata(ZipkinConfig.spanType, msg.getClass.getSimpleName)
        child.addMetadata(ZipkinConfig.spanUniqueClass, pjp.getTarget.toString)

        //child.startSegment(msg.getClass.getSimpleName, ZipkinConfig.internalPrefix + "type", "ZipkinConfig").finish()
        pjp.proceed()

        child.finish()
      }
    } else {
      pjp.proceed()
    }
  }

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