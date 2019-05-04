/*
 * Copyright (c) 2018-2019 The original author or authors
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *        The Eclipse Public License is available at
 *        http://www.eclipse.org/legal/epl-v10.html
 *
 *        The Apache License v2.0 is available at
 *        http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */
package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpSender;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.impl.ProtonSenderImpl;

public class AmqpSenderImpl implements AmqpSender {
  private final ProtonSender sender;
  private final AmqpConnectionImpl connection;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;
  private long remoteCredit = 0;

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSender.class);

  private AmqpSenderImpl(ProtonSender sender, AmqpConnectionImpl connection,
                         Handler<AsyncResult<AmqpSender>> completionHandler) {
    this.sender = sender;
    this.connection = connection;

    sender
      .closeHandler(res -> onClose(sender, res, false))
      .detachHandler(res -> onClose(sender, res, true));

    sender.sendQueueDrainHandler(s -> {
      Handler<Void> dh = null;
      synchronized (AmqpSenderImpl.this) {
        // Update current state of remote credit
        remoteCredit = ((ProtonSenderImpl) sender).getRemoteCredit();

        // check the user drain handler, fire it outside synchronized block if not null
        if (drainHandler != null) {
          dh = drainHandler;
        }
      }

      if (dh != null) {
        dh.handle(null);
      }
    });

    sender.openHandler(done -> {
      if (done.failed()) {
        completionHandler.handle(done.mapEmpty());
      } else {
        connection.register(this);
        completionHandler.handle(Future.succeededFuture(this));
      }
    });

    sender.open();
  }

  private void onClose(ProtonSender sender, AsyncResult<ProtonSender> res, boolean detach) {
    Handler<Throwable> eh = null;
    boolean closeSender = false;

    synchronized (AmqpSenderImpl.this) {
      if (!closed && exceptionHandler != null) {
        eh = exceptionHandler;
      }

      if (!closed) {
        closed = true;
        closeSender = true;
      }
    }

    if (eh != null) {
      if (res.succeeded()) {
        eh.handle(new Exception("Sender closed remotely"));
      } else {
        eh.handle(new Exception("Sender closed remotely with error", res.cause()));
      }
    }

    if (closeSender) {
      if (detach) {
        sender.detach();
      } else {
        sender.close();
      }
    }
  }

  /**
   * Creates a new instance of {@link AmqpSenderImpl}. The created sender is passed into the {@code completionHandler}
   * once opened. This method must be called on the connection context.
   *
   * @param sender            the underlying proton sender
   * @param connection        the connection
   * @param completionHandler the completion handler
   */
  static void create(ProtonSender sender, AmqpConnectionImpl connection,
                     Handler<AsyncResult<AmqpSender>> completionHandler) {
    new AmqpSenderImpl(sender, connection, completionHandler);
  }

  @Override
  public synchronized boolean writeQueueFull() {
    return remoteCredit <= 0;
  }

  @Override
  public AmqpConnection connection() {
    return connection;
  }

  @Override
  public AmqpSender send(AmqpMessage message) {
    return doSend(message, null);
  }

  private AmqpSender doSend(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgmentHandler) {
    AmqpMessage updated;
    if (message.address() == null) {
      updated = AmqpMessage.create(message).address(address()).build();
    } else {
      updated = message;
    }

    Handler<ProtonDelivery> ack = delivery -> {
      Handler<AsyncResult<Void>> handler = acknowledgmentHandler;
      if (acknowledgmentHandler == null) {
        handler = ar -> {
          if (ar.failed()) {
            LOGGER.warn("Message rejected by remote peer", ar.cause());
          }
        };
      }

      switch (delivery.getRemoteState().getType()) {
        case Rejected:
          handler.handle(Future.failedFuture("message rejected (REJECTED"));
          break;
        case Modified:
          handler.handle(Future.failedFuture("message rejected (MODIFIED)"));
          break;
        case Released:
          handler.handle(Future.failedFuture("message rejected (RELEASED)"));
          break;
        case Accepted:
          handler.handle(Future.succeededFuture());
          break;
        default:
          handler.handle(Future.failedFuture("Unsupported delivery type: " + delivery.getRemoteState().getType()));
      }
    };

    synchronized (AmqpSenderImpl.this) {
      // Update the credit tracking. We only need to adjust this here because the sends etc may not be on the context
      // thread and if that is the case we can't use the ProtonSender sendQueueFull method to check that credit has been
      // exhausted following this doSend call since we will have only scheduled the actual send for later.
      remoteCredit--;
    }

    connection.runWithTrampoline(x -> {
      sender.send(updated.unwrap(), ack);

      synchronized (AmqpSenderImpl.this) {
        // Update the credit tracking *again*. We need to reinitialise it here in case the doSend call was performed on
        // a thread other than the client context, to ensure we didn't fall foul of a race between the above pre-send
        // update on that thread, the above send on the context thread, and the sendQueueDrainHandler based updates on
        // the context thread.
        remoteCredit = ((ProtonSenderImpl) sender).getRemoteCredit();
      }
    });
    return this;
  }

  @Override
  public synchronized AmqpSender exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public AmqpSender write(AmqpMessage data) {
    return doSend(data, null);
  }

  @Override
  public AmqpSender write(AmqpMessage data, Handler<AsyncResult<Void>> handler) {
    return doSend(data, handler);
  }

  @Override
  public AmqpSender setWriteQueueMaxSize(int maxSize) {
    // No-op, available sending credit is controlled by recipient peer in AMQP 1.0.
    return this;
  }

  @Override
  public void end() {
    close(null);
  }

  @Override
  public void end(Handler<AsyncResult<Void>> handler) {
    close(handler);
  }

  @Override
  public synchronized AmqpSender drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  @Override
  public AmqpSender sendWithAck(AmqpMessage message, Handler<AsyncResult<Void>> acknowledgementHandler) {
    return doSend(message, acknowledgementHandler);
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    Handler<AsyncResult<Void>> actualHandler;
    if (handler == null) {
      actualHandler = x -> { /* NOOP */ };
    } else {
      actualHandler = handler;
    }

    synchronized (this) {
      if (closed) {
        actualHandler.handle(Future.succeededFuture());
        return;
      }
      closed = true;
    }

    connection.unregister(this);
    connection.runWithTrampoline(x -> {
      if (sender.isOpen()) {
        try {
          sender
            .closeHandler(v -> actualHandler.handle(v.mapEmpty()))
            .close();
        } catch (Exception e) {
          // Somehow closed remotely
          actualHandler.handle(Future.failedFuture(e));
        }
      } else {
        actualHandler.handle(Future.succeededFuture());
      }
    });
  }

  @Override
  public String address() {
    return sender.getRemoteAddress();
  }
}
