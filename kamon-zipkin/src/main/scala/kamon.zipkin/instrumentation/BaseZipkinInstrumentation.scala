package kamon.zipkin.instrumentation

import akka.actor.{ ActorRef, Actor }
import kamon.Kamon
import kamon.trace.{ LevelOfDetail, HierarchyConfig, Tracer }
import kamon.zipkin.ZipkinConfig
import org.aspectj.lang.ProceedingJoinPoint
import org.aspectj.lang.annotation.{ Around, Pointcut, Aspect }

import scala.concurrent.Future

@Aspect
class BaseZipkinInstrumentation {
  import scala.concurrent.ExecutionContext.Implicits.global

  @Pointcut(value = "(!within(kamon..*) || within(kamon.zipkin.instrumentation..*))")
  def filterPointcut() {}

  @Pointcut(value = "execution(* akka.actor.Actor.aroundReceive(..)) && filterPointcut() && args(receive,msg)")
  def receivePointcut(receive: Actor.Receive, msg: Any) {}

  @Pointcut(value = "execution(* akka.pattern.PipeToSupport.PipeableFuture.pipeTo(..)) && filterPointcut() && args(recipient,sender)")
  def pipeToPointcut(recipient: ActorRef, sender: ActorRef) {}

  @Around(value = "receivePointcut(receive,msg)", argNames = "pjp,receive,msg")
  def receiveTracingAround(pjp: ProceedingJoinPoint, receive: Actor.Receive, msg: Any): Unit =
    if (Tracer.currentContext.nonEmpty && Tracer.currentContext.levelOfDetail != LevelOfDetail.MetricsOnly) {
      val (rootToken: String, parentToken: String, parentClass: String, remote: Boolean) = Tracer.currentContext.token.split(HierarchyConfig.tokenSeparator) match {
        case Array(root, parentClassInstance, parent) ⇒ (root, parent, parentClassInstance, true)
        case Array(root, parent)                      ⇒ (root, parent, ZipkinConfig.rootName, true)
        case Array(parent) ⇒ Tracer.currentContext.metadata.get(HierarchyConfig.rootToken) match {
          case Some(token) ⇒ (token, parent, Tracer.currentContext.metadata.getOrElse(HierarchyConfig.spanUniqueClass, ""), false)
          case None ⇒
            Tracer.currentContext.addMetadata(HierarchyConfig.rootToken, parent)
            (parent, parent, ZipkinConfig.rootName, false)
        }
      }

      Tracer.withContext(Kamon.tracer.newChildContext(msg.getClass.getSimpleName)) {
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

        if (!child.metadata.contains(HierarchyConfig.future)) child.finish()
      }
    } else pjp.proceed()

  @Around(value = "pipeToPointcut(recipient,sender)", argNames = "pjp,recipient,sender")
  def pipeToTracingAround(pjp: ProceedingJoinPoint, recipient: ActorRef, sender: ActorRef): Any = {
    val currentContext = Tracer.currentContext
    if (!currentContext.isEmpty) currentContext.addMetadata(HierarchyConfig.future, "future")

    val result = pjp.proceed()
    result match {
      case exec: Future[_] if !currentContext.isEmpty && currentContext.levelOfDetail != LevelOfDetail.MetricsOnly ⇒
        exec onComplete { case _ ⇒ currentContext.finish() }
        result
      case _ if !currentContext.isEmpty && currentContext.levelOfDetail != LevelOfDetail.MetricsOnly ⇒
        currentContext.finish()
        result
      case _ ⇒
        result
    }
  }

}