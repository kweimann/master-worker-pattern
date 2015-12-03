/**
 * Author:  Kuba Weimann
 * Contact: kuba.weimann@gmail.com
 */

package pattern

import java.util
import java.util.Map.Entry

import akka.actor.{Props, Terminated, Actor, ActorRef}
import pattern.WorkDispatcher.Protocol.{RemoveWorkers, Failure, Success, AddWorkers}
import pattern.WorkDispatcher.{WorkFailure, WorkSuccess, WorkDone, WorkerTemplate}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Master-Worker pattern with a cache where completed jobs are stored. Its maximal size can be set using
 * <code>cacheSize</code>. Newly submitted jobs are stored in a hash map and a queue. If at a later time a job
 * will be submitted that has already been put into the hash map, its sender will be added to the list of
 * actors waiting for this job to complete. The amount of workers can be adjusted during runtime by sending
 * appropriate messages (see <code>pattern.WorkDispatcher.Protocol</code>). Finally there is a possibility to control what
 * happens after a worker dies by overriding <code>pattern.WorkDispatcher</code>'s <code>onWorkerTerminated()</code> function.
 *
 * @param createWorker function to create workers. Takes work dispatcher as parameter.
 * @param initialWorkersCount how many workers will be spawned when work dispatcher is created.
 * @param cacheSize maximal size of cache.
 * @tparam A work type
 * @tparam B result type
 */
class WorkDispatcher[A,B](val createWorker: ActorRef => WorkerTemplate[A,B],
                          val initialWorkersCount: Int,
                          val cacheSize: Int) extends Actor {

  // worker -> work
  private val workers = mutable.Map[ActorRef, Option[A]]()
  // work
  private val workQ = mutable.Queue[A]()
  // work -> list of listeners
  private val workMap = mutable.HashMap[A, ListBuffer[ActorRef]]()

  private val cache = new util.LinkedHashMap[A, B](cacheSize, 0.75f, true) {
    override protected def removeEldestEntry(eldest: Entry[A, B]): Boolean = {
      size >= cacheSize
    }
  }

  private var workerId = initialWorkersCount
  private var workersToRemove = 0

  for (i <- 1 to initialWorkersCount) workers += worker(i) -> None

  def onWorkerTerminated(): Unit = {}

  final def receive: Receive = {

    case workDone: WorkDone =>
      val worker = workDone.worker

      workers.get(worker) match {
        // worker belonged to this dispatcher
        case Some(workOption) =>
          // if worker belonged to this dispatcher and sent WorkDone we can assume he had a work to do
          val work = workOption.get
          val listeners = workMap.get(work).get

          val result = workDone match {
            case WorkSuccess(_, reslt: B) =>
              cache.put(work, reslt)
              Success(work, reslt)
            case WorkFailure(_, reason) =>
              Failure(work, reason)
          }

          listeners foreach (_ ! result)
          // remove this work from submitted works
          workMap -= work
          workers.update(worker, None)

          if (workersToRemove > 0) {
            workers -= context.unwatch(worker)
            workersToRemove -= 1
          } else {
            // if there is work to do
            if (workQ.nonEmpty) {
              findWork(worker)
            }
          }

        case _ =>
      }

    case Terminated(worker) =>
      // if the worker belonged to this dispatcher
      workers.get(worker) match {
        // if he was working
        case Some(workOption) if workOption.nonEmpty =>
          val work = workOption.get
          // make sure the listeners will get the results by enqueuing the work
          workQ.enqueue(work)

        case _ =>

      }

      // remove worker from the worker list
      workers -= context.unwatch(worker)
      onWorkerTerminated()

    case AddWorkers(count) =>
      for (i <- 1 to count) {
        val newWorker = worker(workerId + i)
        workers += newWorker -> None
        if (workQ.nonEmpty) {
          findWork(newWorker)
        }
      }
      workerId += count

    case RemoveWorkers(count) =>
      val idleWorkers = workers.filter(_._2.isEmpty).take(count)
      workers --= idleWorkers.keys
      idleWorkers.foreach(worker => context.stop(context.unwatch(worker._1)))
      workersToRemove += Math.max(0, count - idleWorkers.size)

    case work: A =>
      val cached = cache.get(work)

      // check if this work has already been cached
      if (cached != null) {
        sender ! Success(work, cached)
      } else {
        // check if the same work has been submitted
        workMap.get(work) match {
          case Some(listeners) =>
            // work has already been submitted, add listener
            listeners += sender()

          case _ =>
            // it's a new work
            val listeners = ListBuffer(sender())
            workMap += work -> listeners

            if (workQ.isEmpty) {
              // if workQ is empty it's very likely that there is an idle worker
              collectFirstIdleWorker() match {
                case Some(worker) =>
                  worker ! work
                  workers.update(worker, Some(work))
                case _ =>
                  // all workers are busy - add the work to the queue
                  workQ.enqueue(work)
              }
            } else {
              // if workQ is not empty it means that all workers are busy
              // so there is no reason to look for idle one
              workQ.enqueue(work)
            }
        }
      }
  }

  private def worker(i: Int): ActorRef = context.watch(context.actorOf(Props(createWorker(self)), name="Worker-"+i))

  private def collectFirstIdleWorker(): Option[ActorRef] = workers collectFirst {
    case (worker, None) =>
      worker
  }

  private def findWork(worker: ActorRef): Unit = {
    while (workQ.nonEmpty) {
      val work = workQ.dequeue()

      val cached = cache.get(work)

      if (cached != null) {
        // if workQ contains the work it means that workMap contains it as well
        val listeners = workMap.get(work).get
        val result = Success(work, cached)

        listeners foreach (_ ! result)
      } else {
        // work hasn't been done yet. Give it to the worker
        worker ! work
        workers.update(worker, Some(work))
        return
      }
    }
  }
}

object WorkDispatcher {
  private trait WorkDone { val worker: ActorRef }
  private case class WorkSuccess[A](worker: ActorRef, result: A) extends WorkDone
  private case class WorkFailure[A](worker: ActorRef, reason: Throwable) extends WorkDone

  object Protocol {
    // to master
    case class AddWorkers(count: Int)
    case class RemoveWorkers(count: Int)

    // from master
    case class Success[A,B](work: A, result: B)
    case class Failure[A](work: A, reason: Throwable)
  }

  /**
   * Template for implementing workers.
   * @param workDispatcher reference to the work dispatcher
   * @tparam A work type
   * @tparam B result type
   */
  abstract class WorkerTemplate[A,B](val workDispatcher: ActorRef) extends Actor {
    implicit val ec = context.dispatcher

    /**
     * @param work work to do
     * @return result of given work
     */
    def doWork(work: A): B

    final def receive: Receive = {
      case work: A =>
        try {
          val result = doWork(work)
          workDispatcher ! WorkSuccess(self, result)
        } catch {
          case e: Throwable =>
            workDispatcher ! WorkFailure(self, e)
        }
    }
  }
}
