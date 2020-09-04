package com.alice.aef.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that uses a latch to check periodically that a certain condition is met. <br>
 * Example usage: <br>
 * <code>
 * Waiter.getInstance().withFrequency(1000).withTimeout(60000) <br />
 * .until(() -> messageBoxFacade.getCurrentInboxStatuses(REFDATA, INSERTED, true).size() > 0) <br />
 *  .begin();
 * </code>
 *
 * @author Damian Sirbu
 */
public class Waiter {

  private static final Logger log =  LoggerFactory.getLogger(Waiter.class);

  private static final int DEFAULT_COUNT = 1;
  private static final int DEFAULT_TIMEOUT = 60000;
  private static final int DEFAULT_FREQUENCY = 1000;

  private BooleanSupplier booleanSupplier;
  private int frequency = DEFAULT_FREQUENCY;
  private int timeout = DEFAULT_TIMEOUT;
  private int count = DEFAULT_COUNT;

  /**
   * Begins waiting until the specified booleanSupplier condition is satisfied
   * or until the timeout is exceeded
   * @return <code>true</code> if the waiter didn't time out,
   * <code>false</code> otherwise
   */
  public boolean begin() {

    CountDownLatch latch = new CountDownLatch(count);
    Thread workerThread = new Thread(new Worker(latch, booleanSupplier));
    workerThread.start();
    log.debug("Waiter " + this + " started");
    boolean timedOut = false;
    try {
       timedOut = !latch.await(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.warn("Waiter Thread interrupted while awaiting " + e);
    } finally {
      workerThread.interrupt();
    }
    if (timedOut)
      log.warn("Waiter " + this + " timed out after " + timeout + " ms");
    return timedOut;
  }

  /** @author Damian Sirbu */
  private class Worker implements Runnable {

    private CountDownLatch latch;
    private BooleanSupplier booleanSupplier;

    public Worker(CountDownLatch latch, BooleanSupplier booleanSupplier) {
      this.latch = latch;
      this.booleanSupplier = booleanSupplier;
    }

    public void run() {

      try {
        while (latch.getCount() > 0) {

          Thread.sleep(frequency);

          if (booleanSupplier.getAsBoolean()) {
            latch.countDown();
          }
        }

      } catch (InterruptedException e) {
        log.warn(
            "Worker Thread interrupted, probably the condition was not met until timeout expired "
                + e);
      }
    }
  }

  /**
   * @param booleanSupplier
   * @return
   */
  public Waiter until(BooleanSupplier booleanSupplier) {
    this.booleanSupplier = booleanSupplier;
    return this;
  }

  /**
   * @param frequency
   * @return
   */
  public Waiter withFrequency(int frequency) {
    this.frequency = (frequency > 0) ? frequency : DEFAULT_FREQUENCY;
    return this;
  }

  /**
   * @param timeout
   * @return
   */
  public Waiter withTimeout(int timeout) {
    this.timeout = (timeout > 0) ? timeout : DEFAULT_TIMEOUT;
    return this;
  }

  /**
   * @param
   * @return
   */
  public Waiter withCount(int count) {
    this.count = (count > 0) ? count : DEFAULT_COUNT;
    return this;
  }

  /** @return */
  public static Waiter getInstance() {
    return new Waiter();
  }
}
