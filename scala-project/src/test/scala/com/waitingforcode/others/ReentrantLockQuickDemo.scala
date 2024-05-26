package com.waitingforcode.others

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

// It's a demo for the ReentrantLock; not really a test but
// is an interesting code snippet for learning purposes as this mechanism is involved
// in the #processAllAvailable method
object ReentrantLockQuickDemo {

  val reentrantLock = new ReentrantLock(true)

  val condition = reentrantLock.newCondition()
  var shouldStop = false
  def main(args: Array[String]): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        println("Thread 1, Trying to acquire the lock 3 times")
        reentrantLock.lock()
        reentrantLock.lock()
        reentrantLock.lock()
        println("Thread 1, sleeping")
        Thread.sleep(5000L)
        println("Thread 1, unlocked once")
        reentrantLock.unlock()
        println("Thread 1, unlocked twice")
        reentrantLock.unlock()
        Thread.sleep(2000L)
        println("Thread 1, unlocked three times")
        reentrantLock.unlock()
        println("Thread 1, trying to lock...")
        Thread.sleep(5000L)
        reentrantLock.lock()
        println("...Thread 1, locked again")
        shouldStop = true
        println("Signalling the condition after setting the stop to true")
        condition.signalAll()
        reentrantLock.unlock()
      }
    }).start()

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(2000)
        println("Thread 2, Trying to acquire the lock...")
        reentrantLock.lock()
        println("...Thread 2, locked")
        while (!shouldStop) {
          println("Thread 2, starting await")
          condition.await(120, TimeUnit.SECONDS)
          println(s"Thread 2, signalled the condition; shouldStop=${shouldStop}")
        }
        println("Thread 2, unlocked")
        reentrantLock.unlock()
        println("Thread 2, All processed")

      }
    }).start()
  }
}
