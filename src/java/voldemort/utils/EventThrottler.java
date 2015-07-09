/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package voldemort.utils;

import io.tehuti.metrics.MetricConfig;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.metrics.Quota;
import io.tehuti.metrics.QuotaViolationException;
import io.tehuti.metrics.Sensor;
import io.tehuti.metrics.stats.Rate;
import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.annotations.concurrency.NotThreadsafe;

import java.util.concurrent.TimeUnit;

/**
 * A class to throttle Events to a certain rate
 * 
 * This class takes a maximum rate in events/sec and a minimum interval in ms at
 * which to check the rate. The rate is checked every time the interval
 * elapses, and if the events rate exceeds the maximum, the call will block
 * long enough to equalize it.
 * 
 * This is generalized IoThrottler as it existed before, you can use it to
 * throttle on Bytes read/write,number of entries scanned etc.
 * 
 * 
 */
@NotThreadsafe
public class EventThrottler {

    private final static Logger logger = Logger.getLogger(EventThrottler.class);
    private final static long DEFAULT_CHECK_INTERVAL_MS = 50;

    private final Time time;
    private final long ratesPerSecond;
    private final long intervalMs;
    private long startTime;
    private long eventsSeenInLastInterval;

    // Tehuti stuff
    MetricsRepository metricsRepository;
    Rate rate;
    Sensor rateSensor;
    MetricConfig rateConfig;

    public EventThrottler(long ratesPerSecond) {
        this(SystemTime.INSTANCE, ratesPerSecond, DEFAULT_CHECK_INTERVAL_MS);
    }

    public long getRate() {
        return this.ratesPerSecond;
    }

    public EventThrottler(Time time, long ratePerSecond, long intervalMs) {
        this.time = time;
        this.intervalMs = intervalMs;
        this.ratesPerSecond = ratePerSecond;
        this.eventsSeenInLastInterval = 0L;
        this.startTime = 0L;

        // Tehuti init
        this.metricsRepository = new MetricsRepository();
        this.rateConfig = new MetricConfig()
                .quota(Quota.lessThan(ratePerSecond))
                .timeWindow(1, TimeUnit.SECONDS)
                .samples(5);
        this.rateSensor = metricsRepository.sensor("bytes-throughput");
        this.rate = new Rate(TimeUnit.SECONDS);
        rateSensor.add("bytes-throughput.rate", rate, rateConfig);

        if(logger.isDebugEnabled())
            logger.debug("EventThrottler constructed with ratePerSecond = " + ratePerSecond);
    }

    /**
     * Sleeps if necessary to slow down the caller.
     * 
     * @param eventsSeen Number of events seen since last invocation. Basis for
     *        determining whether its necessary to sleep.
     */
    public synchronized void maybeThrottle(int eventsSeen) {
        // TODO: This implements "bang bang" control. This is OK. But, this
        // permits unbounded bursts of activity within the intervalMs. A
        // controller that has more memory and explicitly bounds peak activity
        // within the intervalMs may be better.
        long rateLimit = getRate();

//
//        eventsSeenInLastInterval += eventsSeen;
//        long now = time.getNanoseconds();
//        long elapsedNs = now - startTime;
//        // if we have completed an interval AND we have seen some events, maybe
//        // we should take a little nap
//        if(elapsedNs > intervalMs * Time.NS_PER_MS && eventsSeenInLastInterval > 0) {
//            long eventsPerSec = (eventsSeenInLastInterval * Time.NS_PER_SECOND) / elapsedNs;
//            if(eventsPerSec > rateLimit) {
//                // solve for the amount of time to sleep to make us hit the
//                // correct i/o rate
//                double maxEventsPerMs = rateLimit / (double) Time.MS_PER_SECOND;
//                long elapsedMs = elapsedNs / Time.NS_PER_MS;
//                long sleepTime = Math.round(eventsSeenInLastInterval / maxEventsPerMs - elapsedMs);
//
//                if(logger.isDebugEnabled())
//                    logger.debug("Natural rate is " + eventsPerSec
//                                 + " events/sec max allowed rate is " + rateLimit
//                                 + " events/sec, sleeping for " + sleepTime + " ms to compensate.");
//                if(sleepTime > 0) {
//                    try {
//                        time.sleep(sleepTime);
//                    } catch(InterruptedException e) {
//                        throw new VoldemortException(e);
//                    }
//                }
//            }
//            startTime = now;
//            eventsSeenInLastInterval = 0;
//        }

        // Tehuti-based implementation
//        long ratePerMs = rateLimit / Time.MS_PER_SECOND;
//        long eventsLeftToRecord = eventsSeen;
//        if(logger.isDebugEnabled())
//            logger.debug("EventThrottler.maybeThrottle: eventsSeen = " + eventsSeen +
//                    " , rateLimit = " + rateLimit +
//                    " , ratePerMs = " + ratePerMs);
//        long eventsRecordedInThisIteration;
//        while (eventsLeftToRecord > 0) {
//            try {
//                eventsRecordedInThisIteration = Math.min(eventsLeftToRecord, ratePerMs);
//                eventsLeftToRecord -= eventsRecordedInThisIteration;
//                rateSensor.record(eventsRecordedInThisIteration);
//            } catch (QuotaViolationException e) {
//                if(logger.isDebugEnabled())
//                    logger.debug("EventThrottler over quota: eventsSeen = " + eventsSeen +
//                            " , eventsLeftToRecord = " + eventsLeftToRecord);
//                try {
//                    time.sleep(1);
//                } catch (InterruptedException ie) {
//                    throw new VoldemortException(ie);
//                }
//            }
//        }

        long now = time.getMilliseconds();
        try {
            rateSensor.record(eventsSeen, now);
        } catch (QuotaViolationException e) {
            long currentRate = Math.round(rate.measure(rateConfig, now));
            if (currentRate > rateLimit) {
                long excessRate = currentRate - rateLimit;
                long sleepTimeMs = Math.round(excessRate / (double) rateLimit * Time.MS_PER_SECOND);
                if(logger.isDebugEnabled())
                    logger.debug("Throttler quota exceeded:\n" +
                            "eventsSeen \t= " + eventsSeen + " events/sec,\n" +
                            "currentRate \t= " + currentRate + " events/sec,\n" +
                            "rateLimit \t= " + rateLimit + " events/sec,\n" +
                            "excessRate \t= " + excessRate + " events/sec,\n" +
                            "sleeping for \t" + sleepTimeMs + " ms to compensate.");
                try {
                    time.sleep(sleepTimeMs);
                } catch(InterruptedException ie) {
                    throw new VoldemortException(ie);
                }
            } else if (logger.isDebugEnabled()) {
                logger.debug("Weird. Got QuotaValidationException but measured rate not over rateLimit: " +
                        "currentRate = " + currentRate + " , rateLimit = " + rateLimit);
            }
        }
    }
}
