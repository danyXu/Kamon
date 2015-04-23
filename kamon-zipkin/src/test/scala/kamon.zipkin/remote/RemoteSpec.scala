package kamon.zipkin.remote

import akka.actor._
import akka.remote.RemoteScope
import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.Kamon._
import kamon.testkit.BaseKamonSpec
import kamon.trace.{ HierarchyConfig, Tracer }

class RemoteSpec extends BaseKamonSpec("remote-spec") {

  override implicit lazy val system: ActorSystem = {
    Kamon.start()
    ActorSystem("remoting-spec-local-system", ConfigFactory.parseString(
      """
        |akka {
        |  loggers = ["akka.event.slf4j.Slf4jLogger"]
        |
        |  actor {
        |    provider = "akka.remote.RemoteActorRefProvider"
        |  }
        |  remote {
        |    enabled-transports = ["akka.remote.netty.tcp"]
        |    netty.tcp {
        |      hostname = "127.0.0.1"
        |      port = 2552
        |    }
        |  }
        |}
      """.stripMargin))
  }

  val remoteSystem: ActorSystem = ActorSystem("remoting-spec-remote-system", ConfigFactory.parseString(
    """
      |akka {
      |  loggers = ["akka.event.slf4j.Slf4jLogger"]
      |
      |  actor {
      |    provider = "akka.remote.RemoteActorRefProvider"
      |  }
      |  remote {
      |    enabled-transports = ["akka.remote.netty.tcp"]
      |    netty.tcp {
      |      hostname = "127.0.0.1"
      |      port = 2553
      |    }
      |  }
      |}
    """.stripMargin))

  val RemoteSystemAddress = AddressFromURIString("akka.tcp://remoting-spec-remote-system@127.0.0.1:2553")

  "The Remoting instrumentation" should {
    "propagate the TraceContext's metadata when creating a new remote actor" in {
      Tracer.withContext(newContext("deploy-remote-actor", "deploy-remote-actor-1")) {
        system.actorOf(TraceTokenReplier.remoteProps(Some(testActor), RemoteSystemAddress), "remote-deploy-fixture")
      }

      expectMsg(3)
    }

    "propagate the TraceContext's metadata when sending a message to a remotely deployed actor" in {
      val remoteRef = system.actorOf(TraceTokenReplier.remoteProps(None, RemoteSystemAddress), "remote-message-fixture")

      Tracer.withContext(tracer.newContext("message-remote-actor", Some("message-remote-actor-1"))) {
        remoteRef ! "count"
      }

      expectMsg(3)
    }
  }

}

class TraceTokenReplier(creationTraceContextListener: Option[ActorRef]) extends Actor {
  creationTraceContextListener foreach { recipient ⇒
    recipient ! Tracer.currentContext.token.split(HierarchyConfig.tokenSeparator).length
  }

  def receive = {
    case "count" ⇒
      sender ! Tracer.currentContext.token.split(HierarchyConfig.tokenSeparator).length
  }
}

object TraceTokenReplier {
  def remoteProps(creationTraceContextListener: Option[ActorRef], remoteAddress: Address): Props = {
    Props(new TraceTokenReplier(creationTraceContextListener))
      .withDeploy(Deploy(scope = RemoteScope(remoteAddress)))
  }
}
