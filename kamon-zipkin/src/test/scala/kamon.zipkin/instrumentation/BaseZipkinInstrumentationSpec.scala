package kamon.zipkin.instrumentation

import akka.actor.{ ActorRef, Actor, Props }
import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ TraceInfo, HierarchyConfig, Tracer }

class BaseZipkinInstrumentationSpec extends BaseKamonSpec("base-zipkin-instrumentation-spec") {

  "the base instrumentation of zipkin" should {

    "not do anything if there is no TraceContext when an actor receive a message" in new ZipkinActorFixture {
      zipkinActor ! Tracer.currentContext.token
      expectMsg("no context")
    }

    "create a new TraceContext child if an actor receive a message into a TraceContext" in new ZipkinActorFixture {
      Tracer.withContext(newContext("testKamonZipkin")) {
        zipkinActor ! Tracer.currentContext.token
        expectMsg("child")
      }
    }

    "handle hierarchy by giving parentToken to each child" in new ZipkinChildFixture {
      Tracer.withContext(newContext("testKamonZipkin")) {
        val rootToken = Tracer.currentContext.token
        zipkinChild ! rootToken
        expectMsg(rootToken)
      }
    }

    "handle hierarchy by giving the same rootToken to each actor" in new ZipkinHierarchyFixture {
      Tracer.withContext(newContext("testKamonZipkin")) {
        val rootToken = Tracer.currentContext.token
        zipkinHierarchy ! rootToken
        expectMsg(rootToken)
      }
    }

  }

  trait ZipkinActorFixture {
    val zipkinActor = system.actorOf(Props[ZipkinActor])
  }

  trait ZipkinChildFixture {
    val zipkinChild = system.actorOf(Props[ZipkinChild])
  }

  trait ZipkinHierarchyFixture {
    val zipkinHierarchy = system.actorOf(Props[ZipkinHierarchy])
  }
}

class ZipkinActor() extends Actor {
  def receive = {
    case token: String ⇒
      if (Tracer.currentContext.isEmpty) sender ! "no context"
      else if (token != Tracer.currentContext.token) sender ! "child"
      else sender ! "no child"
  }
}
class ZipkinChild() extends Actor {
  def receive = {
    case s: String ⇒ sender ! Tracer.currentContext.metadata.getOrElse(HierarchyConfig.parentToken, "no parentToken")
  }
}

class ZipkinHierarchy() extends Actor {
  def receive = {
    case s: String ⇒
      val childActor = context.actorOf(Props(classOf[ZipkinHierarchyFollow], sender()))
      childActor ! Tracer.currentContext.token
  }
}
class ZipkinHierarchyFollow(sender: ActorRef) extends Actor {
  def receive = {
    case s: String ⇒ sender ! Tracer.currentContext.metadata.getOrElse(HierarchyConfig.rootToken, "no rootToken")
  }
}