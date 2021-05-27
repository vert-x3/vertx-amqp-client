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

import io.vertx.amqp.*;
import io.vertx.core.*;
import io.vertx.proton.*;
import io.vertx.proton.impl.ProtonConnectionImpl;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Target;
import org.apache.qpid.proton.amqp.messaging.TerminusDurability;
import org.apache.qpid.proton.amqp.messaging.TerminusExpiryPolicy;
import org.apache.qpid.proton.engine.EndpointState;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class AmqpConnectionImpl implements AmqpConnection {

  public static final String PRODUCT = "vertx-amqp-client";
  public static final Symbol PRODUCT_KEY = Symbol.valueOf("product");

  private final AmqpClientOptions options;
  private final AtomicBoolean closed = new AtomicBoolean();
  private final AtomicReference<ProtonConnection> connection = new AtomicReference<>();
  private final Context context;
  private final Promise<Void> closePromise;

  private final List<AmqpSender> senders = new CopyOnWriteArrayList<>();
  private final List<AmqpReceiver> receivers = new CopyOnWriteArrayList<>();
  /**
   * The exception handler, protected by the monitor lock.
   */
  private Handler<Throwable> exceptionHandler;

  AmqpConnectionImpl(Context context, AmqpClientOptions options,
                     ProtonClient proton, Promise<AmqpConnection> connectionHandler) {
    this.options = options;
    this.context = context;
    this.closePromise = Promise.promise();

    runOnContext(x -> connect(
      Objects.requireNonNull(proton, "proton cannot be `null`"),
      Objects.requireNonNull(connectionHandler, "connection handler cannot be `null`"))
    );
  }

  private void connect(ProtonClient proton,
                       Promise<AmqpConnection> connectionPromise) {
    proton
      .connect(options, options.getHost(), options.getPort(), options.getUsername(), options.getPassword(),
        ar -> {
          // Called on the connection context.

          if (ar.succeeded()) {
            ProtonConnection result = ar.result();
            if (!this.connection.compareAndSet(null, result)) {
              connectionPromise.fail("Unable to connect - already holding a connection");
              return;
            }

            Map<Symbol, Object> map = new HashMap<>();
            map.put(AmqpConnectionImpl.PRODUCT_KEY, AmqpConnectionImpl.PRODUCT);
            if (options.getContainerId() != null) {
              this.connection.get().setContainer(options.getContainerId());
            }

            if (options.getConnectionHostname() != null) {
              this.connection.get().setHostname(options.getConnectionHostname());
            }

            this.connection.get()
              .setProperties(map)
              .disconnectHandler(ignored -> {
                try {
                  onDisconnect();
                } finally {
                  closed.set(true);
                  closePromise.tryComplete();
                }
              })
              .closeHandler(x -> {
                // Not expected closing, consider it failed
                try {
                  onDisconnect();

                  result.close();
                  runOnContext(y -> {
                    if(!result.isDisconnected()) {
                      result.disconnect();
                    }
                  });
                } finally {
                  closed.set(true);
                  closePromise.tryComplete();
                }
              })
              .openHandler(conn -> {
                if (conn.succeeded()) {
                  closed.set(false);
                  connectionPromise.complete(this);
                } else {
                  closed.set(true);
                  closePromise.tryComplete();
                  connectionPromise.fail(conn.cause());
                }
              });

            this.connection.get().open();
          } else {
            connectionPromise.fail(ar.cause());
          }
        });
  }

  /**
   * Must be called on context.
   */
  private void onDisconnect() {
    Handler<Throwable> h = null;
    ProtonConnection conn = connection.getAndSet(null);
    synchronized (this) {
      if (exceptionHandler != null) {
        h = exceptionHandler;
      }
    }

    if (h != null && !closed.get()) {
      String message = getErrorMessage(conn);
      h.handle(new Exception(message));
    }
  }

  private String getErrorMessage(ProtonConnection conn) {
    String message = "Connection disconnected";
    if (conn != null) {
      if (conn.getCondition() != null && conn.getCondition().getDescription() != null) {
        message += " - " + conn.getCondition().getDescription();
      } else if (
        conn.getRemoteCondition() != null
          && conn.getRemoteCondition().getDescription() != null) {
        message += " - " + conn.getRemoteCondition().getDescription();
      }
    }
    return message;
  }

  void runOnContext(Handler<Void> action) {
    context.runOnContext(action);
  }

  void runWithTrampoline(Handler<Void> action) {
    if (Vertx.currentContext() == context) {
      action.handle(null);
    } else {
      runOnContext(action);
    }
  }

  /**
   * Must be called on context.
   */
  private boolean isLocalOpen() {
    ProtonConnection conn = this.connection.get();
    return conn != null
      && ((ProtonConnectionImpl) conn).getLocalState() == EndpointState.ACTIVE;
  }

  /**
   * Must be called on context.
   */
  private boolean isRemoteOpen() {
    ProtonConnection conn = this.connection.get();
    return conn != null
      && ((ProtonConnectionImpl) conn).getRemoteState() == EndpointState.ACTIVE;
  }

  @Override
  public synchronized AmqpConnection exceptionHandler(Handler<Throwable> handler) {
    this.exceptionHandler = handler;
    return this;
  }

  @Override
  public AmqpConnection close(Handler<AsyncResult<Void>> done) {
    context.runOnContext(ignored -> {
      ProtonConnection actualConnection = connection.get();
      if (actualConnection == null || closed.get() || (!isLocalOpen() && !isRemoteOpen())) {
        if (done != null) {
          done.handle(Future.succeededFuture());
        }
        return;
      }

      closed.set(true);
      closePromise.tryComplete();

      Future<Void> future = Future.future();
      if (done != null) {
        future.onComplete(done);
      }
      if (actualConnection.isDisconnected()) {
        future.complete();
      } else {
        try {
          actualConnection
            .disconnectHandler(con -> {
              future.tryFail(getErrorMessage(con));
              closed.set(true);
            })
            .closeHandler(res -> {
              closed.set(true);
              if (res.succeeded()) {
                future.tryComplete();
              } else {
                future.tryFail(res.cause());
              }
              actualConnection.disconnect();
            })
            .close();
        } catch (Exception e) {
          future.fail(e);
        }
      }
    });

    return this;
  }

  void unregister(AmqpSender sender) {
    senders.remove(sender);
  }

  void unregister(AmqpReceiver receiver) {
    receivers.remove(receiver);
  }

  @Override
  public AmqpConnection createDynamicReceiver(Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return createReceiver(null, new AmqpReceiverOptions().setDynamic(true), completionHandler);
  }

  @Override
  public AmqpConnection createReceiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    ProtonLinkOptions opts = new ProtonLinkOptions();

    runWithTrampoline(x -> {
      ProtonConnection conn = connection.get();
      if (conn == null) {
        completionHandler.handle(Future.failedFuture("Not connected"));
      } else {
        ProtonReceiver receiver = conn.createReceiver(address, opts);
        new AmqpReceiverImpl(
          Objects.requireNonNull(address, "The address must not be `null`"),
          this, new AmqpReceiverOptions(), receiver,
          Objects.requireNonNull(completionHandler, "The completion handler must not be `null`"));
      }
    });
    return this;
  }

  @Override
  public AmqpConnection createReceiver(String address, AmqpReceiverOptions receiverOptions,
    Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    ProtonLinkOptions opts = new ProtonLinkOptions();
    AmqpReceiverOptions recOpts = receiverOptions == null ? new AmqpReceiverOptions() : receiverOptions;
    opts
      .setDynamic(recOpts.isDynamic())
      .setLinkName(recOpts.getLinkName());

    runWithTrampoline(v -> {
      ProtonConnection conn = connection.get();
      if (conn == null) {
        completionHandler.handle(Future.failedFuture("Not connected"));
      } else {
        ProtonReceiver receiver = conn.createReceiver(address, opts);

        if (receiverOptions != null) {
          if (receiverOptions.getQos() != null) {
            receiver.setQoS(ProtonQoS.valueOf(receiverOptions.getQos().toUpperCase()));
          }

          configureTheSource(recOpts, receiver);
        }

        new AmqpReceiverImpl(address, this, recOpts, receiver, completionHandler);
      }
    });
    return this;
  }

  private void configureTheSource(AmqpReceiverOptions receiverOptions, ProtonReceiver receiver) {
    org.apache.qpid.proton.amqp.messaging.Source source = (org.apache.qpid.proton.amqp.messaging.Source) receiver
      .getSource();

    List<String> capabilities = receiverOptions.getCapabilities();
    if (!capabilities.isEmpty()) {
      source.setCapabilities(capabilities.stream().map(Symbol::valueOf).toArray(Symbol[]::new));
    }

    if (receiverOptions.isDurable()) {
      source.setExpiryPolicy(TerminusExpiryPolicy.NEVER);
      source.setDurable(TerminusDurability.UNSETTLED_STATE);
    }
  }

  @Override
  public AmqpConnection createSender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    Objects.requireNonNull(address, "The address must be set");
    return createSender(address, new AmqpSenderOptions(), completionHandler);
  }

  @Override
  public AmqpConnection createSender(String address, AmqpSenderOptions options,
    Handler<AsyncResult<AmqpSender>> completionHandler) {
    if (address == null && !options.isDynamic()) {
      throw new IllegalArgumentException("Address must be set if the link is not dynamic");
    }

    Objects.requireNonNull(completionHandler, "The completion handler must be set");
    runWithTrampoline(x -> {

      ProtonConnection conn = connection.get();
      if (conn == null) {
        completionHandler.handle(Future.failedFuture("Not connected"));
      } else {
        ProtonSender sender;
        if (options != null) {
          ProtonLinkOptions opts = new ProtonLinkOptions();
          opts.setLinkName(options.getLinkName());
          opts.setDynamic(options.isDynamic());

          sender = conn.createSender(address, opts);
          sender.setAutoDrained(options.isAutoDrained());

          configureTheTarget(options, sender);
        } else {
          sender = conn.createSender(address);
        }

        AmqpSenderImpl.create(sender, this, completionHandler);
      }
    });
    return this;
  }

  private void configureTheTarget(AmqpSenderOptions senderOptions, ProtonSender sender) {
    Target target = (org.apache.qpid.proton.amqp.messaging.Target) sender.getTarget();

    List<String> capabilities = senderOptions.getCapabilities();
    if (!capabilities.isEmpty()) {
      target.setCapabilities(capabilities.stream().map(Symbol::valueOf).toArray(Symbol[]::new));
    }
  }

  @Override
  public AmqpConnection createAnonymousSender(Handler<AsyncResult<AmqpSender>> completionHandler) {
    Objects.requireNonNull(completionHandler, "The completion handler must be set");
    runWithTrampoline(x -> {
      ProtonConnection conn = connection.get();
      if (conn == null) {
        completionHandler.handle(Future.failedFuture("Not connected"));
      } else {
        ProtonSender sender = conn.createSender(null);
        AmqpSenderImpl.create(sender, this, completionHandler);
      }
    });
    return this;
  }

  @Override
  public boolean isDisconnected() {
    ProtonConnection current = this.connection.get();
    if (current != null) {
      return current.isDisconnected();
    } else {
      return true;
    }
  }

  public Future<Void> closeFuture() {
    return closePromise.future();
  }

  /**
   * Allows retrieving the underlying {@link ProtonConnection}.
   *
   * @return the underlying connection
   */
  public ProtonConnection unwrap() {
    return this.connection.get();
  }

  public AmqpClientOptions options() {
    return options;
  }

  void register(AmqpSenderImpl sender) {
    senders.add(sender);
  }

  void register(AmqpReceiverImpl receiver) {
    receivers.add(receiver);
  }
}
