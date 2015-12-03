package pattern

import akka.actor._
import pattern.WorkDispatcher.Protocol.{Failure, Success}

object Example extends App {
  val system = ActorSystem()

  // Implementation of Worker
  class Worker(master: ActorRef) extends WorkDispatcher.WorkerTemplate[Int, Int](master) {
    override def doWork(work: Int): Int = {
      Thread sleep 1000
      work + 1
    }
  }

  // Work Dispatcher
  val workDispatcher = system.actorOf(Props(new WorkDispatcher[Int, Int](new Worker(_), 2, 10)))

  // The recipient of results from work dispatcher
  val listener = system.actorOf(Props(new Actor {
    val start = System.currentTimeMillis()

    override def receive: Receive = {
      case Success(work, result) =>
        println(s"${time()}ms $work -> $result")

      case Failure(work, reason) =>
        println(s"${time()}ms $work -> $reason")
    }

    def time() = System.currentTimeMillis() - start
  }))

  // Send work to the dispatcher. Also set listener as the recipient.
  for (i <- 1 to 10) {
    workDispatcher.tell(i % 5, listener)
  }
}
