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
package io.vertx.amqp.tests;

import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpReceiver;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CloseTest extends BareTestBase {

  @Test(timeout = 20000)
  public void testConsumerCloseCompletionNotification(TestContext context) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncUnregister = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    MockServer server = new MockServer(vertx,
      serverConnection -> handleReceiverOpenSendMessageThenClose(serverConnection, testName, sentContent, context));

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect().onComplete(res -> {
      context.assertTrue(res.succeeded());
      res.result().createReceiver(testName).onComplete(done -> {
        context.assertTrue(done.succeeded());
        AmqpReceiver receiver = done.result();
        receiver.exceptionHandler(x -> exceptionHandlerCalled.set(true));
        receiver.handler(msg -> {
          String amqpBodyContent = msg.bodyAsString();
          context.assertNotNull(amqpBodyContent, "amqp message body content was null");
          context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

          receiver.close().onComplete(context.asyncAssertSuccess(x -> {
            asyncUnregister.complete();

            client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
              asyncShutdown.complete();
            }));
          }));
        });

      });
    });

    try {
      context.assertFalse(exceptionHandlerCalled.get(), "exception handler unexpectedly called");
      asyncUnregister.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  private void handleReceiverOpenSendMessageThenClose(ProtonConnection serverConnection, String testAddress,
    String testContent, TestContext context) {
    // Expect a connection
    serverConnection.openHandler(serverSender -> {
      // Add a close handler
      serverConnection.closeHandler(x -> serverConnection.close());
      serverConnection.open();
    });

    // Expect a session to open, when the receiver is created
    serverConnection.sessionOpenHandler(ProtonSession::open);

    // Expect a sender link open for the receiver
    serverConnection.senderOpenHandler(serverSender -> {
      Source remoteSource = (Source) serverSender.getRemoteSource();
      context.assertNotNull(remoteSource, "source should not be null");
      context.assertEquals(testAddress, remoteSource.getAddress(), "expected given address");
      // Naive test-only handling
      serverSender.setSource(remoteSource.copy());

      // Assume we will get credit, buffer the send immediately
      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(testContent));

      serverSender.send(protonMsg);

      // Add a close handler
      serverSender.closeHandler(x -> serverSender.close());
      serverSender.open();
    });
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyWithErrorCallsExceptionHandler(TestContext context) throws Exception {
    doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(context, true);
  }

  private void doConsumerClosedRemotelyCallsExceptionHandlerTestImpl(TestContext context,
    boolean closeWithError) throws Exception {

    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncExceptionHandlerCalled = context.async();

    final AtomicBoolean msgReceived = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.open();

        // Assume we will get credit, buffer the send immediately
        org.apache.qpid.proton.message.Message protonMsg = Proton.message();
        protonMsg.setBody(new AmqpValue(sentContent));

        // Delay the message, or the connection will be marked as failed immediately.
        vertx.setTimer(500, x -> {
          serverSender.send(protonMsg);
          // Mark it closed server side
          if (closeWithError) {
            serverSender.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));
          }
          serverSender.close();

        });
      });
    });

    // === Client consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    this.client.connect().onComplete(context.asyncAssertSuccess(res -> {
      res.createReceiver(testName).onComplete(context.asyncAssertSuccess(
        receiver -> {
          receiver.handler(msg -> {
            String content = msg.bodyAsString();
            context.assertNotNull(content, "amqp message body content was null");
            context.assertEquals(sentContent, content, "amqp message body not as expected");
            msgReceived.set(true);
          });
          receiver.exceptionHandler(ex -> {
            context.assertNotNull(ex, "expected exception");
            context.assertTrue(ex instanceof Exception, "expected exception");
            if (closeWithError) {
              context.assertNotNull(ex.getCause(), "expected cause");
            } else {
              context.assertNull(ex.getCause(), "expected no cause");
            }

            context.assertTrue(msgReceived.get(), "expected msg to be received first");
            asyncExceptionHandlerCalled.complete();

            client.close().onComplete(context.asyncAssertSuccess(v -> {
              asyncShutdown.complete();
            }));
          });
        }));
    }));

    try {
      asyncExceptionHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerClosedRemotelyCallsEndHandler(TestContext context) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncEndHandlerCalled = context.async();

    final AtomicBoolean msgReceived = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> {
          serverConnection.close();
        });

        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        serverSender.open();

        vertx.setTimer(500, x -> {
          // Assume we will get credit, buffer the send immediately
          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));

          serverSender.send(protonMsg);

          // Mark it closed server side
          serverSender.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));

          serverSender.close();
        });
      });
    });

    // === Client consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      res.createReceiver(testName).onComplete(context.asyncAssertSuccess(consumer -> {
          consumer.handler(msg -> {
            context.assertNotNull(msg.bodyAsString(), "message body was null");
            String amqpBodyContent = msg.bodyAsString();
            context.assertNotNull(amqpBodyContent, "amqp message body content was null");
            context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

            msgReceived.set(true);
          });
          consumer.endHandler(x -> {
            context.assertTrue(msgReceived.get(), "expected msg to be received first");
            asyncEndHandlerCalled.complete();
            client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
              asyncShutdown.complete();
            }));
          });
        }));
    }));

    try {
      asyncEndHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConsumerClosedLocallyDoesNotCallEndHandler(TestContext context) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the receiver is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a sender link open for the receiver
      serverConnection.senderOpenHandler(serverSender -> {
        Source remoteSource = (Source) serverSender.getRemoteSource();
        context.assertNotNull(remoteSource, "source should not be null");
        context.assertEquals(testName, remoteSource.getAddress(), "expected given address");
        // Naive test-only handling
        serverSender.setSource(remoteSource.copy());

        vertx.setTimer(500, x -> {
          // Assume we will get credit, buffer the send immediately
          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));
          serverSender.send(protonMsg);
          // Add a close handler
          serverSender.closeHandler(y -> serverSender.close());
        });

        serverSender.open();
      });
    });

    // === client consumer handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setPort(server.actualPort()).setHost("localhost");
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      res.createReceiver(testName).onComplete(context.asyncAssertSuccess(
        consumer -> {
          consumer.endHandler(x -> context.fail("should not call end handler"));
          // Attach handler.
          consumer.handler(msg -> {
            context.assertNotNull(msg.bodyAsString(), "message body was null");
            context.assertEquals(sentContent, msg.bodyAsString(), "amqp message body not as expected");

            consumer.close().onComplete(context.asyncAssertSuccess(x -> {
              // closing complete, schedule shutdown, give chance for end handler to run, so we can verify it didn't.
              vertx.setTimer(50, y -> {
                client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
                  asyncShutdown.complete();
                }));
              });
            }));
          });
        }));
    }));

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConnectionClosedLocallyDisconnectsTransport(TestContext context) throws Exception {
    final Async asyncShutdown = context.async();
    final Async asyncCloseHandlerCalled = context.async();
    final Async asyncDisconnectHandlerCalled = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection, open it, expect it to close, then disconnect
      serverConnection.openHandler(serverSender -> {
        serverConnection.open();

        serverConnection.closeHandler(y -> {
          serverConnection.close();
          asyncCloseHandlerCalled.complete();
        });

        serverConnection.disconnectHandler(y -> {
          context.assertTrue(asyncCloseHandlerCalled.isCompleted());
          asyncDisconnectHandlerCalled.complete();
        });
      });
    });

    // === Client handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {

      conn.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
        asyncShutdown.complete();
      }));
    }));

    try {
      asyncCloseHandlerCalled.awaitSuccess();
      asyncDisconnectHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConnectionClosedLocallyDoesNotCallExceptionHandler(TestContext context) throws Exception {
    final Async asyncShutdown = context.async();
    final CountDownLatch latch = new CountDownLatch(1);

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> {
          serverConnection.close();
          serverConnection.disconnect();
        });

        serverConnection.open();
      });
    });

    // === Client handling ====

    AmqpClientOptions options = new AmqpClientOptions()
        .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      conn.exceptionHandler(
          x -> {
            latch.countDown();
          });

      conn.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
        asyncShutdown.complete();
      }));
    }));

    try {
      asyncShutdown.awaitSuccess();
      context.assertFalse(latch.await(50, TimeUnit.MILLISECONDS), "exception handler should not have fired");
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConnectionClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doConnectionEndHandlerCalledTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testConnectionDisconnectedCallsExceptionHandler(TestContext context) throws Exception {
    doConnectionEndHandlerCalledTestImpl(context, true);
  }

  private void doConnectionEndHandlerCalledTestImpl(TestContext context, boolean disconnect) throws Exception {
    final Async asyncShutdown = context.async();
    final Async asyncEndHandlerCalled = context.async();
    final CountDownLatch latch = new CountDownLatch(1);

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnection.open();

        // Remotely close/disconnect the connection after a delay
        vertx.setTimer(100, x -> {
          if (disconnect) {
            serverConnection.disconnect();
          } else {
            serverConnection.close();
            serverConnection.disconnect();
          }
        });
      });
    });

    // === Client handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      res.exceptionHandler(
        x -> {
          if(asyncEndHandlerCalled.isCompleted()) {
            latch.countDown();
          }
          asyncEndHandlerCalled.complete();

          client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
            asyncShutdown.complete();
          }));
        });
    }));

    try {
      asyncEndHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();

      if(!disconnect) {
        context.assertFalse(latch.await(50, TimeUnit.MILLISECONDS), "exception handler should not have fired more than once");
      }
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testConnectionClosedRemotelySendsCloseInResponseAndDisconnects(TestContext context) throws Exception {
    final Async asyncExeptionHandlerCalled = context.async();
    final Async asyncShutdown = context.async();
    final Async asyncCloseHandlerCalled = context.async();
    final Async asyncDisconnectHandlerCalled = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        serverConnection.open();

        // Remotely close the connection after a delay, but don't disconnect
        vertx.setTimer(20, x -> {
          serverConnection.closeHandler(y -> {
            asyncCloseHandlerCalled.complete();

            context.assertTrue(asyncExeptionHandlerCalled.isCompleted());
            context.assertFalse(asyncDisconnectHandlerCalled.isCompleted());
          });

          serverConnection.disconnectHandler(y -> {
            asyncDisconnectHandlerCalled.complete();

            context.assertTrue(asyncExeptionHandlerCalled.isCompleted());
            context.assertTrue(asyncCloseHandlerCalled.isCompleted());
          });

          serverConnection.close();
        });
      });
    });

    // === Client handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(conn -> {
      conn.exceptionHandler(
        x -> {
          asyncExeptionHandlerCalled.complete();
          context.assertFalse(asyncCloseHandlerCalled.isCompleted());
          context.assertFalse(asyncDisconnectHandlerCalled.isCompleted());

          client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
            asyncShutdown.complete();
          }));
        });
    }));

    try {
      asyncExeptionHandlerCalled.awaitSuccess();
      asyncShutdown.awaitSuccess();
      asyncCloseHandlerCalled.awaitSuccess();
      asyncDisconnectHandlerCalled.awaitSuccess();
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testCloseClientThatWithoutConnection(TestContext context) {
    Async async = context.async();

    AmqpClient client = AmqpClient.create(new AmqpClientOptions().setHost("unused"));
    client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
      async.complete();
    }));

    async.awaitSuccess();
  }
}
