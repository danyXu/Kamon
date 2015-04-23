package kamon.zipkin.instrumentation

import akka.actor.Actor
import kamon.Kamon
import kamon.akka.remote.RemoteConfig
import kamon.trace.Tracer
import kamon.zipkin.ZipkinConfig
import org.aspectj.lang.{ JoinPoint, ProceedingJoinPoint }
import org.aspectj.lang.annotation.{ Before, Around, Pointcut, Aspect }
import tv.teads.gaia.logging.LazyLogging

@Aspect
class BaseZipkinInstrumentation extends LazyLogging {

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

  @Before(value = "enableZipkinPointcut()")
  def beforeMethodsWithPointcut(jp: JoinPoint) = {
    if (!Tracer.currentContext.isEmpty) {
      val args = jp.getArgs.foldLeft("") { (a, b) ⇒
        if (a.isEmpty)
          a + b.getClass.getSimpleName
        else
          a + ", " + b.getClass.getSimpleName
      }
      val txt = jp.getSignature.getName + "(" + args + ")"
      logger.info(txt)
      val segment = Tracer.currentContext.startSegment(txt, "", "").finish()
    }
  }

  /*
  @Before("execution (* *(..)) && !within(kamon..*) && !within(akka..*) && !within(scala..*) && within(tv.teads..*)")
  def before(jp: JoinPoint) = {
    if (!Tracer.currentContext.isEmpty && Tracer.currentContext.metadata.get(RemoteConfig.rootToken).nonEmpty) {
      val ctx = Tracer.currentContext
      // ctx.startSegment(jp.getSignature.getName, "", "").finish()
      logger.info("test987 " + jp.getSignature.getName)
    }
  }
  */

}