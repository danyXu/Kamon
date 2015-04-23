package kamon.zipkin.instrumentation

import akka.actor.{ ActorRef, Actor, Props }
import kamon.akka.remote.RemoteConfig
import kamon.testkit.BaseKamonSpec
import kamon.trace.Tracer

class BaseZipkinInstrumentationSpec extends BaseKamonSpec("zipkin-instrumentation-spec") {

  "the Kamon Zipkin module" should {

    "not do anything if there is no TraceContext when an actor receive a message" in {
      val zipkinActor = system.actorOf(ZipkinActors.props())
      zipkinActor ! Tracer.currentContext.token
      expectMsg("no child")
    }

    "create a new TraceContext child if an actor receive a message into a TraceContext" in {
      Tracer.withContext(newContext("testKamonZipkin")) {
        val token = Tracer.currentContext.token
        val zipkinActor = system.actorOf(ZipkinActors.props())
        zipkinActor ! token
        expectMsg("child")
      }
    }

    "handle hierarchy by giving parentToken to each child" in {
      Tracer.withContext(newContext("testKamonZipkin")) {
        val rootToken = Tracer.currentContext.token
        val zipkinActor = system.actorOf(ZipkinActors.propsChild())
        zipkinActor ! rootToken
        expectMsg(rootToken)
      }
    }

    "handle hierarchy by giving the same rootToken to each actor" in {
      Tracer.withContext(newContext("testKamonZipkin")) {
        val rootToken = Tracer.currentContext.token
        val zipkinActor = system.actorOf(ZipkinActors.propsHierarchy())
        zipkinActor ! rootToken
        expectMsg(rootToken)
      }
    }

    "log method executions in classes with EnableZipkin annotation" in {
      Tracer.withContext(newContext("testKamonZipkin")) {
        val hello = new TestAnnotation
        hello.hello() should be("hello")
      }
    }
  }
}

class ZipkinActor() extends Actor {
  def receive = {
    case s: String ⇒
      val child = Tracer.currentContext
      if (s != Tracer.currentContext.token) sender ! "child"
      else sender ! "no child"
  }
}
class ZipkinActorHierarchy() extends Actor {
  def receive = {
    case s: String ⇒
      val childActor = context.actorOf(ZipkinActors.propsHierarchy2(sender()))
      childActor ! Tracer.currentContext.token
  }
}
class ZipkinActorHierarchy2(sender: ActorRef) extends Actor {
  def receive = {
    case s: String ⇒
      val ctx = Tracer.currentContext
      sender ! ctx.metadata.getOrElse(RemoteConfig.rootToken, "no rootToken")
  }
}
class ZipkinActorChild() extends Actor {
  def receive = {
    case s: String ⇒
      val ctx = Tracer.currentContext
      sender ! ctx.metadata.getOrElse(RemoteConfig.parentToken, "no parentToken")
  }
}

object ZipkinActors {
  def props(): Props = Props(classOf[ZipkinActor])
  def propsHierarchy(): Props = Props(classOf[ZipkinActorHierarchy])
  def propsHierarchy2(sender: ActorRef): Props = Props(classOf[ZipkinActorHierarchy2], sender)
  def propsChild(): Props = Props(classOf[ZipkinActorChild])
}

@EnableZipkin
class TestAnnotation {
  def hello(): String = {
    "hello"
  }
}