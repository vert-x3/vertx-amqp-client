package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.ext.amqp.AmqpClient;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import org.apache.qpid.proton.amqp.Symbol;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public class AmqpClientImpl implements AmqpClient {

  private final Vertx vertx;
  private final ProtonClient proton;
  private final AmqpClientOptions options;

  private final List<AmqpConnection> connections = new CopyOnWriteArrayList<>();
  private final boolean mustCloseVertxOnClose;
  private volatile Context context;

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
    context = vertx.getOrCreateContext();
    context.runOnContext(x -> connection(connectionHandler, context).run());
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

    CompositeFuture.all(actions).setHandler(done -> {
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

  private Runnable connection(Handler<AsyncResult<AmqpConnection>> connectionHandler, Context context) {
    return () ->
      proton.connect(options, options.getHost(), options.getPort(), options.getUsername(), options.getPassword(), connection -> {
        if (connection.succeeded()) {
          Map<Symbol, Object> map = new HashMap<>();
          map.put(AmqpConnectionImpl.PRODUCT_KEY, AmqpConnectionImpl.PRODUCT);
          ProtonConnection result = connection.result();
          if (options.getContainerId() != null) {
            result.setContainer(options.getContainerId());
          }
          if (options.getVirtualHost() != null) {
            result.setHostname(options.getVirtualHost());
          }

          result
            .setProperties(map)
            .openHandler(conn -> {
              if (conn.succeeded()) {

                AmqpConnection amqp = new AmqpConnectionImpl(options, context, conn.result());
                connections.add(amqp);
                context.runOnContext(x -> connectionHandler.handle(Future.succeededFuture(amqp)));
              } else {
                context.runOnContext(x -> connectionHandler.handle(conn.mapEmpty()));
              }
            });
          result.open();
        } else {
          context.runOnContext(x -> connectionHandler.handle(connection.mapEmpty()));
        }
      });
  }
}
