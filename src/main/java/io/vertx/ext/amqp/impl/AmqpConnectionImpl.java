package io.vertx.ext.amqp.impl;

import io.vertx.core.*;
import io.vertx.ext.amqp.*;
import io.vertx.proton.*;
import io.vertx.proton.impl.ProtonConnectionImpl;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.engine.EndpointState;

import java.util.*;
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

  private final List<AmqpSender> senders = new CopyOnWriteArrayList<>();
  private final List<AmqpReceiver> receivers = new CopyOnWriteArrayList<>();

  private final ReplyManager replyManager;

  /**
   * Access protected by monitor lock.
   */
  private Handler<Void> endHandler;

  AmqpConnectionImpl(Vertx vertx, Context context, AmqpClientImpl client, AmqpClientOptions options,
                     ProtonClient proton, Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    this.options = options;
    this.context = context;
    this.replyManager = new ReplyManager(vertx, context, this,
      options.isReplyEnabled(), options.getReplyTimeout());

    runOnContext(x -> connect(client,
      Objects.requireNonNull(proton, "proton cannot be `null`"),
      Objects.requireNonNull(connectionHandler, "connection handler cannot be `null`"))
    );
  }

  private void connect(AmqpClientImpl client, ProtonClient proton, Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    proton
      .connect(options, options.getHost(), options.getPort(), options.getUsername(), options.getPassword(),
        ar -> {
          if (ar.succeeded()) {
            if (!this.connection.compareAndSet(null, ar.result())) {
              connectionHandler.handle(Future.failedFuture("Unable to connect - already holding a connection"));
              return;
            }

            Map<Symbol, Object> map = new HashMap<>();
            map.put(AmqpConnectionImpl.PRODUCT_KEY, AmqpConnectionImpl.PRODUCT);
            if (options.getContainerId() != null) {
              this.connection.get().setContainer(options.getContainerId());
            }

            if (options.getVirtualHost() != null) {
              this.connection.get().setHostname(options.getVirtualHost());
            }

            this.connection.get()
              .setProperties(map)
              .disconnectHandler(ignored -> onEnd())
              .closeHandler(ignored -> {
                try {
                  onDisconnect();
                } finally {
                  onEnd();
                }
              })
              .openHandler(conn -> {
                if (conn.succeeded()) {
                  client.register(this);
                  this.replyManager.initialize().setHandler(res -> {
                    if (res.failed()) {
                      connectionHandler.handle(Future.failedFuture(res.cause()));
                    } else {
                      closed.set(false);
                      connectionHandler.handle(Future.succeededFuture(this));
                    }
                  });
                } else {
                  runWithTrampoline(x -> connectionHandler.handle(conn.mapEmpty()));
                }
              });

            this.connection.get().open();
          } else {
            runWithTrampoline(x -> connectionHandler.handle(ar.mapEmpty()));
          }
        });
  }

  private void onDisconnect() {
    ProtonConnection conn = connection.getAndSet(null);
    if (conn != null) {
      try {
        conn.close();
      } finally {
        conn.disconnect();
      }
    }
  }

  private void onEnd() {
    Handler<Void> handler;
    synchronized (this) {
      handler = endHandler;
      endHandler = null;
    }
    if (handler != null && !closed.get()) {
      handler.handle(null);
    }
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

  public ReplyManager replyManager() {
    return replyManager;
  }

  private boolean isLocalOpen() {
    ProtonConnection conn = this.connection.get();
    return conn != null
      && ((ProtonConnectionImpl) conn).getLocalState() == EndpointState.ACTIVE;
  }

  private boolean isRemoteOpen() {
    ProtonConnection conn = this.connection.get();
    return conn != null
      && ((ProtonConnectionImpl) conn).getRemoteState() == EndpointState.ACTIVE;
  }

  @Override
  public synchronized void endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
  }

  @Override
  public AmqpConnection close(Handler<AsyncResult<Void>> done) {
    List<Future> futures = new ArrayList<>();

    ProtonConnection actualConnection = connection.get();
    if (actualConnection == null || (closed.get() && (!isLocalOpen() && !isRemoteOpen()))) {
      if (done != null) {
        done.handle(Future.succeededFuture());
      }
      return this;
    } else {
      closed.set(true);
    }
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
      if (done != null) {
        future.setHandler(done);
      }
      if (actualConnection.isDisconnected()) {
        future.complete();
      } else {
        try {
          actualConnection
            .closeHandler(cleanup ->
              runWithTrampoline(x -> {
                onDisconnect();
                future.handle(cleanup.mapEmpty());
              }))
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
  public AmqpConnection receiver(String address, Handler<AmqpMessage> handler, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    return receiver(address, null, handler, completionHandler);
  }

  @Override
  public AmqpConnection receiver(String address, Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    ProtonLinkOptions opts = new ProtonLinkOptions();
    ProtonReceiver receiver = connection.get().createReceiver(address, opts);

    runWithTrampoline(x -> new AmqpReceiverImpl(
      Objects.requireNonNull(address, "The address must not be `null`"),
      this, receiver, null,
      Objects.requireNonNull(completionHandler, "The completion handler must not be `null`")));
    return this;
  }

  @Override
  public AmqpConnection receiver(String address, AmqpReceiverOptions receiverOptions, Handler<AmqpMessage> handler,
                                 Handler<AsyncResult<AmqpReceiver>> completionHandler) {
    ProtonLinkOptions opts = new ProtonLinkOptions();
    if (receiverOptions != null) {
      opts = new ProtonLinkOptions()
        .setDynamic(receiverOptions.isDynamic())
        .setLinkName(receiverOptions.getLinkName());
    }
    ProtonReceiver receiver = connection.get().createReceiver(address, opts);

    if (receiverOptions != null && receiverOptions.getQos() != null) {
      receiver.setQoS(ProtonQoS.valueOf(receiverOptions.getQos().toUpperCase()));
    }

    runWithTrampoline(x -> new AmqpReceiverImpl(address, this, receiver, handler, completionHandler));
    return this;
  }

  @Override
  public AmqpConnection sender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    ProtonSender sender = connection.get().createSender(address);
    AmqpSenderImpl.create(sender, this, completionHandler);
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

    ProtonSender sender = connection.get().createSender(address, linkOptions);
    AmqpSenderImpl.create(sender, this, completionHandler);
    return this;
  }

  @Override
  public AmqpConnection closeHandler(Handler<AmqpConnection> remoteCloseHandler) {
    this.connection.get().closeHandler(pc -> {
      if (remoteCloseHandler != null) {
        runWithTrampoline(x -> remoteCloseHandler.handle(this));
      }
    });
    return this;
  }

  ProtonConnection unwrap() {
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
