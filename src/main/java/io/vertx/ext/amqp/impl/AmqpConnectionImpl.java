package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.ext.amqp.*;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonLinkOptions;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

import java.util.Objects;

public class AmqpConnectionImpl implements AmqpConnection {

  private final AmqpClientOptions options;
  private final ProtonConnection connection;
  private final Context context;

  AmqpConnectionImpl(AmqpClientOptions options, Context context, ProtonConnection connection) {
    this.options = options;
    this.connection = connection;
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
  public AmqpConnection receiver(String address, AmqpLinkOptions receiverOptions, Handler<AmqpMessage> handler,
                                 Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    Objects.requireNonNull(address, "The address must not be `null`");
    Objects.requireNonNull(handler, "The message handler must not be `null`");
    Objects.requireNonNull(completionHandler, "The completion handler must not be `null`");
    ProtonLinkOptions options = new ProtonLinkOptions();
    if (receiverOptions != null) {
      options = new ProtonLinkOptions().setDynamic(receiverOptions.isDynamicAddress())
        .setLinkName(receiverOptions.getName());
    }
    ProtonReceiver receiver = connection.createReceiver(address, options);
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
    ProtonLinkOptions options = new ProtonLinkOptions();
    if (senderOptions != null) {
      options = new ProtonLinkOptions()
        .setDynamic(senderOptions.isDynamicAddress())
        .setLinkName(senderOptions.getName());
    }

    ProtonSender sender = connection.createSender(address, options);
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
