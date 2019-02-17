package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.ext.amqp.*;
import io.vertx.proton.*;
import org.apache.qpid.proton.amqp.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class AmqpConnectionImpl implements AmqpConnection {

  public static final String PRODUCT = "vertx-amqp-client";
  public static final Symbol PRODUCT_KEY = Symbol.valueOf("product");

  private final AmqpClientOptions options;
  private final ProtonConnection connection;
  private final Context context;

  private List<AmqpSender> senders = new CopyOnWriteArrayList<>();
  private List<AmqpReceiver> receivers = new CopyOnWriteArrayList<>();

  private final ReplyManager replyManager;

  AmqpConnectionImpl(Vertx vertx, AmqpClientOptions options, Context context, ProtonConnection connection) {
    this.options = options;
    this.connection = Objects.requireNonNull(connection, "connection cannot be `null`");
    this.context = context;

    this.replyManager = new ReplyManager(vertx, context, this,
      options.isReplyEnabled(), options.getReplyTimeout());
  }

  public Future<Void> init() {
    return this.replyManager.initialize();
  }

  public void runOnContext(Handler<Void> action) {
    context.runOnContext(action);
  }

  public void runWithTrampoline(Handler<Void> action) {
    if (Vertx.currentContext() == context) {
      action.handle(null);
    } else {
      runOnContext(action);
    }
  }

  public ReplyManager replyManager() {
    return replyManager;
  }

  @Override
  public AmqpConnection close(Handler<AsyncResult<Void>> done) {
    List<Future> futures = new ArrayList<>();
    futures.add(replyManager.close());
    synchronized (this) {
      senders.forEach(sender -> {
        Future<Void> future = Future.future();
        futures.add(future);
        sender.close(future);
      });
      receivers.forEach(receiver -> {
        Future<Void> future = Future.future();
        futures.add(future);
        receiver.close(future);
      });
    }

    CompositeFuture.join(futures).setHandler(result -> {
      Future<Void> future = Future.future();
      connection
        .closeHandler(closed ->
          runWithTrampoline(x -> future.handle(closed.mapEmpty())))
        .close();
      if (done != null) {
        future.setHandler(done);
      }
    });
    return this;
  }

  void unregister(AmqpSender sender) {
    synchronized (this) {
      // Sender is close explicitly.
      senders.remove(sender);
    }
  }

  void unregister(AmqpReceiver receiver) {
    synchronized (this) {
      // Receiver is close explicitly.
      receivers.remove(receiver);
    }
  }

  @Override
  public AmqpConnection receiver(String address, Handler<AmqpMessage> handler, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return receiver(address, null, handler, completionHandler);
  }

  @Override
  public AmqpConnection receiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    Objects.requireNonNull(address, "The address must not be `null`");
    Objects.requireNonNull(completionHandler, "The completion handler must not be `null`");
    ProtonLinkOptions opts = new ProtonLinkOptions();
    ProtonReceiver receiver = connection.createReceiver(address, opts)
      .setAutoAccept(true);

    runWithTrampoline(x -> {
      new AmqpReceiverImpl(address, this, receiver, null, completionHandler);
    });
    return this;
  }

  // TODO Allow creating a receiver just passing the handler

  @Override
  public AmqpConnection receiver(String address, AmqpReceiverOptions receiverOptions, Handler<AmqpMessage> handler,
                                 Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    ProtonLinkOptions opts = new ProtonLinkOptions();
    if (receiverOptions != null) {
      opts = new ProtonLinkOptions()
        .setDynamic(receiverOptions.isDynamic())
        .setLinkName(receiverOptions.getLinkName());
    }
    ProtonReceiver receiver = connection.createReceiver(address, opts)
      .setAutoAccept(true);

    if (receiverOptions != null  && receiverOptions.getQos() != null) {
        receiver.setQoS(ProtonQoS.valueOf(receiverOptions.getQos().toUpperCase()));
    }

    runWithTrampoline(x -> {
      new AmqpReceiverImpl(address, this, receiver, handler, completionHandler);
    });
    return this;
  }

  @Override
  public AmqpConnection sender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    ProtonSender sender = connection.createSender(address);
    openSender(completionHandler, sender);
    return this;
  }

  private void openSender(Handler<AsyncResult<AmqpSender>> completionHandler, ProtonSender sender) {
    context.runOnContext(x -> {
      sender
        .openHandler(done -> {
          if (done.failed()) {
            completionHandler.handle(done.mapEmpty());
          } else {
            AmqpSenderImpl result = new AmqpSenderImpl(done.result(), this, context);
            senders.add(result);
            completionHandler.handle(Future.succeededFuture(result));
          }
        })
        .open();
    });
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
    openSender(completionHandler, sender);
    return this;
  }

  @Override
  public AmqpConnection closeHandler(Handler<AmqpConnection> remoteCloseHandler) {
    this.connection.closeHandler(pc -> {
      if (remoteCloseHandler != null) {
        context.runOnContext(x -> {
          remoteCloseHandler.handle(this);
        });
      }
    });
    return this;
  }

  public ProtonConnection unwrap() {
    return this.connection;
  }
}
