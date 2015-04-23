package kamon.zipkin.instrumentation

import akka.actor.Actor
import kamon.Kamon
import kamon.akka.remote.RemoteConfig
import kamon.trace.Tracer
import kamon.zipkin.{ Zipkin, ZipkinConfig }
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Around, Pointcut, Aspect }

@Aspect
class BaseZipkinInstrumentation {

  @Pointcut(value = "execution(* akka.actor.Actor.aroundReceive(..)) && (!within(kamon..*) || within(kamon.zipkin.instrumentation..*)) && args(receive,msg)", argNames = "receive,msg")
  def aroundReceivePointcut(receive: Actor.Receive, msg: Any) {}

  @Pointcut("execution((* || Any) (@kamon.zipkin.instrumentation.EnableZipkin *).*(..)) && !execution((* || Any) akka..*(..)) && !execution(Actor.Receive *(..))")
  def enableZipkinPointcut() = {}

  @Around(value = "aroundReceivePointcut(receive,msg)", argNames = "pjp,receive,msg")
  def receiveTracingAround(pjp: ProceedingJoinPoint, receive: Actor.Receive, msg: Any): Unit = {
    if (!Tracer.currentContext.isEmpty) {
      val tokenEncoded = Tracer.currentContext.token.split(RemoteConfig.tokenSeparator)
      val parentToken = tokenEncoded.last
      var rootToken = Tracer.currentContext.metadata.getOrElse(RemoteConfig.rootToken, parentToken)
      if (tokenEncoded.length > 1) rootToken = tokenEncoded.head
      if (rootToken == parentToken) Tracer.currentContext.addMetadata(RemoteConfig.rootToken, rootToken)

      Tracer.withContext(Kamon.tracer.newContext(msg.getClass.getSimpleName)) {
        val child = Tracer.currentContext
        child.startTimestamp

        child.addMetadata(RemoteConfig.rootToken, rootToken)
        child.addMetadata(RemoteConfig.parentToken, parentToken)
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

  @Around(value = "enableZipkinPointcut()")
  def aroundMethodsWithPointcut(pjp: ProceedingJoinPoint): Any = {
    if (!Kamon(Zipkin).config.getBoolean("trace-methods") && !Tracer.currentContext.isEmpty) {
      val args = pjp.getArgs.foldLeft("") { (a, b) ⇒
        if (a.isEmpty)
          a + b.getClass.getSimpleName
        else
          a + ", " + b.getClass.getSimpleName
      }
      val txt = pjp.getSignature.getName + "(" + args + ")"
      val segment = Tracer.currentContext.startSegment(txt, "", "")
      val r = pjp.proceed()
      segment.finish()
      r
    } else {
      pjp.proceed()
    }
  }

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

}