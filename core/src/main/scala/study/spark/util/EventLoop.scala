package study.spark.util

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import study.spark.Logging


/**
 * An event loop to receive events from the caller and process all events in the event thread. It
 * will start an exclusive event thread to process all events.
 *
 * Note: The event queue will grow indefinitely. So subclasses should make sure `onReceive` can
 * handle events in time to avoid the potential OOM.
 */
private[spark] abstract class EventLoop[E](name: String) extends Logging {


  private val eventQueue: BlockingQueue[E] = new LinkedBlockingDeque[E]()

  /**
   * Put the event into the event queue. The event thread will process it later.
   */
  def post(event: E): Unit = {
    eventQueue.put(event)
  }


}
