package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.ext.amqp.AmqpClient;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.proton.ProtonClient;

import java.util.Objects;

public class AmqpClientImpl implements AmqpClient {

  private final Vertx vertx;
  private final ProtonClient proton;
  private final AmqpClientOptions options;

  public AmqpClientImpl(Vertx vertx, AmqpClientOptions options) {
    this.vertx = vertx;
    this.options = options;
    this.proton = ProtonClient.create(vertx);
  }

  @Override
  public AmqpClient connect(Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    Objects.requireNonNull(options.getHost(), "Host must be set");
    Objects.requireNonNull(connectionHandler, "Handler must not be null");
    Context context = vertx.getOrCreateContext();
    context.runOnContext(x -> connection(connectionHandler, context).run());
    return this;
  }

  private Runnable connection(Handler<AsyncResult<AmqpConnection>> connectionHandler, Context context) {
    return () ->
      proton.connect(options, options.getHost(), options.getPort(), options.getUsername(), options.getPassword(), connection -> {
        if (connection.succeeded()) {
          connection.result().openHandler(conn -> {
            if (conn.succeeded()) {
              if (options.getContainerId() != null) {
                connection.result().setContainer(options.getContainerId());
              }
              AmqpConnection amqp = new AmqpConnectionImpl(options, context, conn.result());
              context.runOnContext(x -> connectionHandler.handle(Future.succeededFuture(amqp)));
            } else {
              context.runOnContext(x -> connectionHandler.handle(conn.mapEmpty()));
            }
          });
          connection.result().open();
        } else {
          context.runOnContext(x -> connectionHandler.handle(connection.mapEmpty()));
        }
      });
  }
}
