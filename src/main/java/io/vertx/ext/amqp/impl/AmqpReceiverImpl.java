/**
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

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.VertxException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.amqp.AmqpMessage;
import io.vertx.ext.amqp.AmqpReceiver;
import io.vertx.proton.ProtonReceiver;

import java.util.ArrayDeque;
import java.util.Queue;

public class AmqpReceiverImpl implements AmqpReceiver {


  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpReceiverImpl.class);


  private final String address;
  private final ProtonReceiver receiver;
  private final AmqpConnectionImpl connection;
  private final Queue<AmqpMessageImpl> buffered = new ArrayDeque<>();

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
   * @param address           the address
   * @param connection        the connection
   * @param receiver          the underlying proton receiver
   * @param handler           the handler
   * @param completionHandler called when the receiver is opened
   */
  public AmqpReceiverImpl(
    String address,
    AmqpConnectionImpl connection,
    ProtonReceiver receiver,
    Handler<AmqpMessage> handler, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    this.address = address;
    this.receiver = receiver;
    this.connection = connection;
    this.handler = handler;
    int maxBufferedMessages = connection.options().getMaxBufferedMessages();
    if (maxBufferedMessages > 0) {
      this.initialCredit = maxBufferedMessages;
    }

    // Disable auto-accept and automated prefetch, we manage disposition and credit
    // manually to allow for delayed handler registration and pause/resume functionality.
    this.receiver
      .setAutoAccept(false)
      .setPrefetch(0);

    this.receiver.handler((delivery, message) -> handleMessage(new AmqpMessageImpl(message, delivery)));
    if (this.handler != null) {
      handler(this.handler);
    }

    this.receiver.closeHandler(res -> {
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
        receiver.close();
      }
    });

    this.receiver
      .openHandler(res -> {
        if (res.failed()) {
          completionHandler.handle(res.mapEmpty());
        } else {
          this.connection.register(this);
          completionHandler.handle(Future.succeededFuture(this));
        }
      });

    this.receiver.open();
  }

  private void handleMessage(AmqpMessageImpl message) {
    boolean schedule = false;

    message.setReplyManager(connection.replyManager());

    synchronized (this) {
      if (handler != null && demand > 0L && buffered.isEmpty()) {
        if (demand != Long.MAX_VALUE) {
          demand--;
        }
      } else if (handler != null && demand > 0L) {
        // Buffered messages present, deliver the oldest of those instead
        buffered.add(message);
        message = buffered.poll();
        if (demand != Long.MAX_VALUE) {
          demand--;
        }

        // Schedule a delivery for the next buffered message
        schedule = true;
      } else {
        // Buffer message until we aren't paused
        buffered.add(message);
      }
    }
    deliverMessageToHandler(message);

    // schedule next delivery if appropriate, after earlier delivery to allow chance to pause etc.
    if (schedule) {
      scheduleBufferedMessageDelivery();
    }
  }

  @Override
  public synchronized AmqpReceiverImpl exceptionHandler(Handler<Throwable> handler) {
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

  private void deliverMessageToHandler(AmqpMessageImpl message) {
    Handler<AmqpMessage> h;
    synchronized (this) {
      h = handler;
    }

    h.handle(message);
    message.delivered();
    this.receiver.flow(1);
  }

  private void scheduleBufferedMessageDelivery() {
    boolean schedule;

    synchronized (this) {
      schedule = !buffered.isEmpty() && demand > 0L;
    }

    if (schedule) {
      connection.runOnContext(v -> {
        AmqpMessageImpl message = null;

        synchronized (this) {
          if (demand > 0L) {
            if (demand != Long.MAX_VALUE) {
              demand--;
            }
            message = buffered.poll();
          }
        }

        if (message != null) {
          // Delivering outside the synchronized block
          deliverMessageToHandler(message);

          // Schedule a delivery for a further buffered message if any
          scheduleBufferedMessageDelivery();
        }
      });
    }
  }

  @Override
  public String address() {
    return address;
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
      if (this.receiver.isOpen()) {
        try {
          receiver
            .closeHandler(done -> actualHandler.handle(done.mapEmpty()))
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

  ProtonReceiver unwrap() {
    return receiver;
  }
}
