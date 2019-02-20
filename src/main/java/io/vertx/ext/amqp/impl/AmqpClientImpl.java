package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.ext.amqp.AmqpClient;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.proton.ProtonClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class AmqpClientImpl implements AmqpClient {

  private final Vertx vertx;
  private final ProtonClient proton;
  private final AmqpClientOptions options;

  private final List<AmqpConnection> connections = new CopyOnWriteArrayList<>();
  private final boolean mustCloseVertxOnClose;

  public AmqpClientImpl(Vertx vertx, AmqpClientOptions options, boolean mustCloseVertxOnClose) {
    this.vertx = vertx;
    this.options = options;
    this.proton = ProtonClient.create(vertx);
    this.mustCloseVertxOnClose = mustCloseVertxOnClose;
  }

  @Override
  public AmqpClient connect(Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    Objects.requireNonNull(options.getHost(), "Host must be set");
    Objects.requireNonNull(connectionHandler, "Handler must not be null");
    new AmqpConnectionImpl(vertx, vertx.getOrCreateContext(), this, options, proton, connectionHandler);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> handler) {
    List<Future> actions = new ArrayList<>();
    for (AmqpConnection connection : connections) {
      Future<Void> future = Future.future();
      connection.close(future);
      actions.add(future);
    }

    CompositeFuture.join(actions).setHandler(done -> {
      connections.clear();
      if (mustCloseVertxOnClose) {
        vertx.close(x -> {
          if (done.succeeded() && x.succeeded()) {
            if (handler != null) {
              handler.handle(Future.succeededFuture());
            }
          } else {
            if (handler != null) {
              handler.handle(Future.failedFuture(done.failed() ? done.cause() : x.cause()));
            }
          }
        });
      } else if (handler != null) {
        handler.handle(done.mapEmpty());
      }
    });

  }

  synchronized void register(AmqpConnectionImpl connection) {
    connections.add(connection);
  }
}
