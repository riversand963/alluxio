/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.audit;

import alluxio.Configuration;
import alluxio.PropertyKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import javax.annotation.concurrent.ThreadSafe;

/**
 * {@link AsyncUserAccessAuditLogWriter} writes user access audit log entries asynchronously.
 *
 * TODO(yanqin) investigate other lock-free queues, e.g. Lmax disruptor used by Lmax and log4j 2
 */
@ThreadSafe
public final class AsyncUserAccessAuditLogWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncUserAccessAuditLogWriter.class);
  private static final Logger AUDIT_LOG =
      LoggerFactory.getLogger("AUDIT_LOG");
  private volatile boolean mStopped;

  /**
   * Constructs an {@link AsyncUserAccessAuditLogWriter} instance.
   */
  public AsyncUserAccessAuditLogWriter() {
    int queueCapacity = Configuration.getInt(PropertyKey.MASTER_AUDIT_LOGGING_QUEUE_CAPACITY);
    LOG.info("Audit logging queue capacity is {}.", queueCapacity);
    mStopped = true;
  }

  /**
   * Starts {@link AsyncUserAccessAuditLogWriter}.
   */
  public synchronized void start() {
    if (mStopped) {
      mStopped = false;
      enableAsyncAuditAppender();
      LOG.info("AsyncUserAccessAuditLogWriter thread started.");
    }
  }

  /**
   * Stops {@link AsyncUserAccessAuditLogWriter}.
   */
  public synchronized void stop() {
    if (!mStopped) {
      mStopped = true;
      LOG.info("AsyncUserAccessAuditLogWriter stopped.");
    }
  }

  /**
   * Appends an {@link AuditContext}.
   *
   * @param context the audit context to append
   * @return true if append operation succeeds, false otherwise
   */
  public boolean append(AuditContext context) {
    AUDIT_LOG.info(context.toString());
    return true;
  }

  private void enableAsyncAuditAppender() {
    org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("AUDIT_LOG");
    List<org.apache.log4j.Appender> appenders = Collections.list(logger.getAllAppenders());
    // failsafe against trying to async it more than once
    if (!appenders.isEmpty() && !(appenders.get(0) instanceof org.apache.log4j.AsyncAppender)) {
      org.apache.log4j.AsyncAppender asyncAppender = new org.apache.log4j.AsyncAppender();
      // change logger to have an async appender containing all the
      // previously configured appenders
      for (org.apache.log4j.Appender appender : appenders) {
        logger.removeAppender(appender);
        asyncAppender.addAppender(appender);
      }
      logger.addAppender(asyncAppender);
      LOG.info("Async appender added.");
    }
  }
}
