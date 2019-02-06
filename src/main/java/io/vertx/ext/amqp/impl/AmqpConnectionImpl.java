package io.vertx.ext.amqp.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.amqp.*;
import io.vertx.proton.*;

import java.util.Objects;

public class AmqpConnectionImpl implements AmqpConnection {

  private final AmqpClientOptions options;
  private final ProtonConnection connection;
  private final Context context;

  AmqpConnectionImpl(AmqpClientOptions options, Context context, ProtonConnection connection) {
    this.options = options;
    this.connection = Objects.requireNonNull(connection, "connection cannot be `null`");
    this.context = context;
  }

  @Override
  public AmqpConnection close() {
    connection.close();
    return this;
  }

  @Override
  public AmqpConnection receiver(String address, Handler<AmqpMessage> handler, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return receiver(address, null, handler, completionHandler);
  }

  // TODO Allow creating a receiver just passing the handler

  @Override
  public AmqpConnection receiver(String address, AmqpReceiverOptions receiverOptions, Handler<AmqpMessage> handler,
                                 Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    Objects.requireNonNull(address, "The address must not be `null`");
    Objects.requireNonNull(handler, "The message handler must not be `null`");
    Objects.requireNonNull(completionHandler, "The completion handler must not be `null`");
    ProtonLinkOptions opts = new ProtonLinkOptions();
    if (receiverOptions != null) {
      opts = new ProtonLinkOptions()
        .setDynamic(receiverOptions.isDynamic())
        .setLinkName(receiverOptions.getLinkName());
    }
    ProtonReceiver receiver = connection.createReceiver(address, opts)
      .setAutoAccept(true);

    if (receiverOptions != null) {
      receiver.setQoS(ProtonQoS.valueOf(receiverOptions.getQos().toUpperCase()));
    }

    receiver
      .handler((delivery, message) -> handler.handle(new AmqpMessageBuilder(message).build()))
      .openHandler(res -> {
        if (res.failed()) {
          completionHandler.handle(res.mapEmpty());
        } else {
          completionHandler.handle(Future.succeededFuture(new AmqpReceiverImpl(address, res.result())));
        }
      })
      .open();
    return this;
  }

  @Override
  public AmqpConnection sender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    ProtonSender sender = connection.createSender(address);
    sender
      .openHandler(done -> {
        if (done.failed()) {
          completionHandler.handle(done.mapEmpty());
        } else {
          completionHandler.handle(Future.succeededFuture(new AmqpSenderImpl(done.result())));
        }
      })
      .open();
    return this;
  }

  @Override
  public AmqpConnection sender(String address, AmqpLinkOptions senderOptions, Handler<AsyncResult<AmqpSender>> completionHandler) {
    ProtonLinkOptions linkOptions = new ProtonLinkOptions();
    if (senderOptions != null) {
      linkOptions = new ProtonLinkOptions()
        .setDynamic(senderOptions.isDynamicAddress())
        .setLinkName(senderOptions.getName());
    }

    ProtonSender sender = connection.createSender(address, linkOptions);
    sender
      .openHandler(done -> {
        if (done.failed()) {
          completionHandler.handle(done.mapEmpty());
        } else {
          completionHandler.handle(Future.succeededFuture(new AmqpSenderImpl(done.result())));
        }
      })
      .open();
    return this;
  }

  @Override
  public AmqpConnection closeHandler(Handler<AmqpConnection> remoteCloseHandler) {
    this.connection.closeHandler(pc -> {
      if (remoteCloseHandler != null) {
        remoteCloseHandler.handle(this);
      }
    });
    return this;
  }

}
