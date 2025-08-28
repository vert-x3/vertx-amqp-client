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
package io.vertx.amqp.impl;

import io.vertx.amqp.AmqpConnection;
import io.vertx.amqp.AmqpMessage;
import io.vertx.amqp.AmqpSender;
import io.vertx.core.*;
import io.vertx.core.internal.logging.Logger;
import io.vertx.core.internal.logging.LoggerFactory;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonSender;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.transport.DeliveryState;

public class AmqpSenderImpl implements AmqpSender {
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSender.class);
  private final ProtonSender sender;
  private final AmqpConnectionImpl connection;
  private boolean closed;
  private Handler<Throwable> exceptionHandler;
  private Handler<Void> drainHandler;
  private long remoteCredit = 0;

  private AmqpSenderImpl(ProtonSender sender, AmqpConnectionImpl connection,
                         Completable<AmqpSender> completionHandler) {
    this.sender = sender;
    this.connection = connection;

    sender
      .closeHandler(res -> onClose(sender, res, false))
      .detachHandler(res -> onClose(sender, res, true));

    sender.sendQueueDrainHandler(s -> {
      Handler<Void> dh = null;
      synchronized (AmqpSenderImpl.this) {
        // Update current state of remote credit
        remoteCredit = sender.getRemoteCredit();

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
        completionHandler.complete(null, done.cause());
      } else {
        connection.register(this);
        completionHandler.succeed(this);
      }
    });

    sender.open();
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
                     Completable<AmqpSender> completionHandler) {
    new AmqpSenderImpl(sender, connection, completionHandler);
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

  private AmqpSender doSend(AmqpMessage message, Completable<Void> acknowledgmentHandler) {
    Handler<ProtonDelivery> ack = delivery -> {
      DeliveryState remoteState = delivery.getRemoteState();

      Completable<Void> handler = acknowledgmentHandler;
      if (acknowledgmentHandler == null) {
        handler = (res, err) -> {
          if (err != null) {
            LOGGER.warn("Message rejected by remote peer", err);
          }
        };
      }

      if (remoteState == null) {
        handler.fail("Unknown message state");
        return;
      }

      switch (remoteState.getType()) {
        case Rejected:
          handler.fail("message rejected (REJECTED): " + ((Rejected) remoteState).getError());
          break;
        case Modified:
          handler.fail("message rejected (MODIFIED)");
          break;
        case Released:
          handler.fail("message rejected (RELEASED)");
          break;
        case Accepted:
          handler.succeed();
          break;
        default:
          handler.fail("Unsupported delivery type: " + remoteState.getType());
      }
    };

    synchronized (AmqpSenderImpl.this) {
      // Update the credit tracking. We only need to adjust this here because the sends etc may not be on the context
      // thread and if that is the case we can't use the ProtonSender sendQueueFull method to check that credit has been
      // exhausted following this doSend call since we will have only scheduled the actual send for later.
      remoteCredit--;
    }

    connection.runWithTrampoline(x -> {
      AmqpMessage updated;
      if (message.address() == null) {
        updated = AmqpMessage.create(message).address(address()).build();
      } else {
        updated = message;
      }

      sender.send(updated.unwrap(), ack);

      synchronized (AmqpSenderImpl.this) {
        // Update the credit tracking *again*. We need to reinitialise it here in case the doSend call was performed on
        // a thread other than the client context, to ensure we didn't fall foul of a race between the above pre-send
        // update on that thread, the above send on the context thread, and the sendQueueDrainHandler based updates on
        // the context thread.
        remoteCredit = sender.getRemoteCredit();
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
  public Future<Void> write(AmqpMessage data) {
    Promise<Void> promise = Promise.promise();
    doSend(data, promise);
    return promise.future();
  }

  @Override
  public AmqpSender setWriteQueueMaxSize(int maxSize) {
    // No-op, available sending credit is controlled by recipient peer in AMQP 1.0.
    return this;
  }

  @Override
  public Future<Void> end() {
    Promise<Void> promise = Promise.promise();
    close(promise);
    return promise.future();
  }

  @Override
  public synchronized AmqpSender drainHandler(Handler<Void> handler) {
    drainHandler = handler;
    return this;
  }

  public AmqpSender sendWithAck(AmqpMessage message, Promise<Void> acknowledgementHandler) {
    return doSend(message, acknowledgementHandler);
  }

  @Override
  public Future<Void> sendWithAck(AmqpMessage message) {
    Promise<Void> promise = Promise.promise();
    sendWithAck(message, promise);
    return promise.future();
  }

  public void close(Completable<Void> handler) {
    Completable<Void> actualHandler;
    if (handler == null) {
      actualHandler = (res, err) -> { /* NOOP */ };
    } else {
      actualHandler = handler;
    }

    synchronized (this) {
      if (closed) {
        actualHandler.succeed();
        return;
      }
      closed = true;
    }

    connection.unregister(this);
    connection.runWithTrampoline(x -> {
      if (sender.isOpen()) {
        try {
          sender
            .closeHandler(v -> actualHandler.complete(null, v.cause()))
            .close();
        } catch (Exception e) {
          // Somehow closed remotely
          actualHandler.fail(e);
        }
      } else {
        actualHandler.succeed();
      }
    });
  }

  @Override
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    close(promise);
    return promise.future();
  }

  @Override
  public String address() {
    return sender.getRemoteAddress();
  }

  @Override
  public long remainingCredits() {
    return sender.getRemoteCredit();
  }

  @Override
  public ProtonSender unwrap() {
    return sender;
  }
}
