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

public class AmqpConnectionImpl implements AmqpConnection {

  public static final String PRODUCT = "vertx-amqp-client";
  public static final Symbol PRODUCT_KEY = Symbol.valueOf("product");

  private final AmqpClientOptions options;
  private AtomicBoolean closed = new AtomicBoolean();
  private ProtonConnection connection;
  private final Context context;

  private List<AmqpSender> senders = new CopyOnWriteArrayList<>();
  private List<AmqpReceiver> receivers = new CopyOnWriteArrayList<>();

  private final ReplyManager replyManager;
  private Handler<Void> endHandler;

  AmqpConnectionImpl(Vertx vertx, Context context, AmqpClientImpl client, AmqpClientOptions options,
                     ProtonClient proton, Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    this.options = options;
    Objects.requireNonNull(connectionHandler, "connection handler cannot be `null`");
    Objects.requireNonNull(proton, "proton cannot be `null`");
    this.context = context;
    this.replyManager = new ReplyManager(vertx, context, this,
      options.isReplyEnabled(), options.getReplyTimeout());

    runOnContext(x -> connect(client, proton, connectionHandler));
  }

  private void connect(AmqpClientImpl client, ProtonClient proton, Handler<AsyncResult<AmqpConnection>> connectionHandler) {
    proton
      .connect(options, options.getHost(), options.getPort(), options.getUsername(), options.getPassword(),
        ar -> {
          if (ar.succeeded()) {
            this.connection = ar.result();

            Map<Symbol, Object> map = new HashMap<>();
            map.put(AmqpConnectionImpl.PRODUCT_KEY, AmqpConnectionImpl.PRODUCT);
            if (options.getContainerId() != null) {
              this.connection.setContainer(options.getContainerId());
            }

            if (options.getVirtualHost() != null) {
              this.connection.setHostname(options.getVirtualHost());
            }

            this.connection
              .setProperties(map)
              .disconnectHandler(closeResult -> onEnd())
              .closeHandler(closeResult -> {
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

            this.connection.open();
          } else {
            runWithTrampoline(x -> connectionHandler.handle(ar.mapEmpty()));
          }
        });
  }

  private synchronized void onDisconnect() {
    ProtonConnection conn = connection;
    connection = null;
    if (conn != null) {
      try {
        // Does nothing if already closed
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

  private boolean isLocalOpen(ProtonConnection connection) {
    return connection != null
      && ((ProtonConnectionImpl) connection).getLocalState() == EndpointState.ACTIVE;
  }

  private boolean isRemoteOpen(ProtonConnection connection) {
    return connection != null
      && ((ProtonConnectionImpl) connection).getRemoteState() == EndpointState.ACTIVE;
  }

  @Override
  public void endHandler(Handler<Void> endHandler) {
    this.endHandler = endHandler;
  }

  @Override
  public AmqpConnection close(Handler<AsyncResult<Void>> done) {
    List<Future> futures = new ArrayList<>();

    ProtonConnection actualConnection;
    synchronized (this) {
      actualConnection = connection;
    }

    if (actualConnection == null || (closed.get() && (!isLocalOpen(actualConnection) && !isRemoteOpen(actualConnection)))) {
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
    ProtonReceiver receiver = connection.createReceiver(address, opts);

    runWithTrampoline(x -> new AmqpReceiverImpl(address, this, receiver, null, completionHandler));
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
    ProtonReceiver receiver = connection.createReceiver(address, opts)
      .setAutoAccept(true);

    if (receiverOptions != null && receiverOptions.getQos() != null) {
      receiver.setQoS(ProtonQoS.valueOf(receiverOptions.getQos().toUpperCase()));
    }

    runWithTrampoline(x -> new AmqpReceiverImpl(address, this, receiver, handler, completionHandler));
    return this;
  }

  @Override
  public AmqpConnection sender(String address, Handler<AsyncResult<AmqpSender>> completionHandler) {
    ProtonSender sender = connection.createSender(address);
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

    ProtonSender sender = connection.createSender(address, linkOptions);
    AmqpSenderImpl.create(sender, this, completionHandler);
    return this;
  }

  @Override
  public AmqpConnection closeHandler(Handler<AmqpConnection> remoteCloseHandler) {
    this.connection.closeHandler(pc -> {
      if (remoteCloseHandler != null) {
        runWithTrampoline(x -> remoteCloseHandler.handle(this));
      }
    });
    return this;
  }

  public ProtonConnection unwrap() {
    return this.connection;
  }

  public AmqpClientOptions options() {
    return options;
  }

  public synchronized void register(AmqpSenderImpl sender) {
    senders.add(sender);
  }

  public void register(AmqpReceiverImpl receiver) {
    receivers.add(receiver);
  }
}
