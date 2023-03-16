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
import io.vertx.amqp.AmqpReceiver;
import io.vertx.amqp.AmqpReceiverOptions;
import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.VertxException;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.proton.ProtonReceiver;

import java.util.ArrayDeque;
import java.util.Queue;

public class AmqpReceiverImpl implements AmqpReceiver {

  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpReceiverImpl.class);
  private final ProtonReceiver receiver;
  private final AmqpConnectionImpl connection;
  private final Queue<AmqpMessageImpl> buffered = new ArrayDeque<>();
  private final boolean durable;
  private final boolean autoAck;
  /**
   * The address.
   * Not final because for dynamic link the address is set when the createReceiver is opened.
   */
  private String address;
  private Handler<AmqpMessage> handler;
  private long demand = Long.MAX_VALUE;
  private boolean closed;
  private Handler<Void> endHandler;
  private Handler<Throwable> exceptionHandler;
  private boolean initialCreditGiven;
  private int initialCredit = 1000;

  /**
   * Creates a new instance of {@link AmqpReceiverImpl}.
   * This method must be called on the connection context.
   *
   * @param address           the address, may be {@code null} for dynamic links
   * @param connection        the connection
   * @param options           the receiver options, must not be {@code null}
   * @param receiver          the underlying proton createReceiver
   * @param completionHandler called when the createReceiver is opened
   */
  AmqpReceiverImpl(
    String address,
    AmqpConnectionImpl connection,
    AmqpReceiverOptions options,
    ProtonReceiver receiver,
    Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    this.address = address;
    this.receiver = receiver;
    this.connection = connection;
    this.durable = options.isDurable();
    this.autoAck = options.isAutoAcknowledgement();
    int maxBufferedMessages = options.getMaxBufferedMessages();
    if (maxBufferedMessages > 0) {
      this.initialCredit = maxBufferedMessages;
    }

    // Disable auto-accept and automated prefetch, we manage disposition and credit
    // manually to allow for delayed handler registration and pause/resume functionality.
    this.receiver
      .setAutoAccept(false)
      .setPrefetch(0);

    this.receiver.handler((delivery, message) -> handleMessage(new AmqpMessageImpl(message, delivery, connection)));

    this.receiver.closeHandler(res -> {
      onClose(address, receiver, res, false);
    })
      .detachHandler(res -> {
        onClose(address, receiver, res, true);
      });

    this.receiver
      .openHandler(res -> {
        if (res.failed()) {
          completionHandler.handle(res.mapEmpty());
        } else {
          this.connection.register(this);
          synchronized (this) {
            if (this.address == null) {
              this.address = res.result().getRemoteAddress();
            }
          }
          completionHandler.handle(Future.succeededFuture(this));
        }
      });

    this.receiver.open();
  }

  private void onClose(String address, ProtonReceiver receiver, AsyncResult<ProtonReceiver> res, boolean detach) {
    Handler<Void> endh = null;
    Handler<Throwable> exh = null;
    boolean closeReceiver = false;

    synchronized (AmqpReceiverImpl.this) {
      if (!closed && endHandler != null) {
        endh = endHandler;
      } else if (!closed && exceptionHandler != null) {
        exh = exceptionHandler;
      }

      if (!closed) {
        closed = true;
        closeReceiver = true;
      }
    }

    if (endh != null) {
      endh.handle(null);
    } else if (exh != null) {
      if (res.succeeded()) {
        exh.handle(new VertxException("Consumer closed remotely"));
      } else {
        exh.handle(new VertxException("Consumer closed remotely with error", res.cause()));
      }
    } else {
      if (res.succeeded()) {
        LOGGER.warn("Consumer for address " + address + " unexpectedly closed remotely");
      } else {
        LOGGER.warn("Consumer for address " + address + " unexpectedly closed remotely with error", res.cause());
      }
    }

    if (closeReceiver) {
      if (detach) {
        receiver.detach();
      } else {
        receiver.close();
      }
    }
  }

  private void handleMessage(AmqpMessageImpl message) {
    boolean schedule = false;

    Handler<AmqpMessage> h;
    synchronized (this) {
      h = handler;
      if(h == null || demand == 0L) {
        // Buffer message until we aren't paused
        buffered.add(message);
        return;
      }

      if(!buffered.isEmpty()) {
        // Buffered messages present, deliver the oldest of those instead
        buffered.add(message);
        message = buffered.poll();
        // Schedule a delivery for the next buffered message
        schedule = true;
      }
      if (demand != Long.MAX_VALUE) {
        demand--;
      }
    }
    deliverMessageToHandler(h, message);

    // schedule next delivery if appropriate, after earlier delivery to allow chance to pause etc.
    if(schedule) {
      scheduleBufferedMessageDelivery();
    }
  }

  @Override
  public synchronized AmqpReceiver exceptionHandler(Handler<Throwable> handler) {
    exceptionHandler = handler;
    return this;
  }

  @Override
  public AmqpReceiver handler(@Nullable Handler<AmqpMessage> handler) {
    int creditToFlow = 0;
    boolean schedule = false;

    synchronized (this) {
      this.handler = handler;
      if (handler != null) {
        schedule = true;

        // Flow initial credit if needed
        if (!initialCreditGiven) {
          initialCreditGiven = true;
          creditToFlow = initialCredit;
        }
      }
    }

    if (creditToFlow > 0) {
      final int c = creditToFlow;
      connection.runWithTrampoline(v -> receiver.flow(c));
    }

    if (schedule) {
      scheduleBufferedMessageDelivery();
    }

    return this;
  }

  @Override
  public synchronized AmqpReceiverImpl pause() {
    demand = 0L;
    return this;
  }

  @Override
  public synchronized AmqpReceiverImpl fetch(long amount) {
    if (amount > 0) {
      demand += amount;
      if (demand < 0L) {
        demand = Long.MAX_VALUE;
      }
      scheduleBufferedMessageDelivery();
    }
    return this;
  }

  @Override
  public synchronized AmqpReceiverImpl resume() {
    return fetch(Long.MAX_VALUE);
  }

  @Override
  public synchronized AmqpReceiverImpl endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
    return this;
  }

  private void deliverMessageToHandler(Handler<AmqpMessage> h, AmqpMessageImpl message) {
    try {
      h.handle(message);
      if (autoAck) {
        message.accepted();
      }
    } catch (Exception e) {
      LOGGER.error("Unable to dispatch the AMQP message", e);
      if (autoAck) {
        message.rejected();
      }
    }

    this.receiver.flow(1);
  }

  private void scheduleBufferedMessageDelivery() {
    boolean schedule;

    synchronized (this) {
      schedule = !buffered.isEmpty() && demand > 0L;
    }

    if (schedule) {
      connection.runOnContext(v -> {
        Handler<AmqpMessage> h;
        AmqpMessageImpl message = null;

        synchronized (this) {
          h = handler;
          if (h != null && demand > 0L) {
            message = buffered.poll();
            if (demand != Long.MAX_VALUE && message != null) {
              demand--;
            }
          }
        }

        if (message != null) {
          // Delivering outside the synchronized block
          deliverMessageToHandler(h, message);

          // Schedule a delivery for a further buffered message if any
          scheduleBufferedMessageDelivery();
        }
      });
    }
  }

  @Override
  public synchronized String address() {
    return address;
  }

  @Override
  public AmqpConnection connection() {
    return connection;
  }

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
      if (this.receiver.isOpen()) {
        try {
          if (isDurable()) {
            receiver.detachHandler(done -> actualHandler.handle(done.mapEmpty()))
              .detach();
          } else {
            receiver
              .closeHandler(done -> actualHandler.handle(done.mapEmpty()))
              .close();
          }
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
  public Future<Void> close() {
    Promise<Void> promise = Promise.promise();
    close(promise);
    return promise.future();
  }

  @Override
  public ProtonReceiver unwrap() {
    return receiver;
  }

  private synchronized boolean isDurable() {
    return durable;
  }
}
