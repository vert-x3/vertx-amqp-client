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
import io.vertx.amqp.AmqpMessage;
import io.vertx.amqp.AmqpSenderOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSession;

import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Modified;
import org.apache.qpid.proton.amqp.messaging.Rejected;
import org.apache.qpid.proton.amqp.messaging.Released;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.Target;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SenderTest extends BareTestBase {

  private static final String ACCEPT = "accept";
  private static final String RELEASE = "release";
  private static final String MODIFY_FAILED = "modify-failed";
  private static final String MODIFY_FAILED_U_H = "modify-failed-u-h";
  private static final String REJECT = "reject";

  private static final String DEFAULT = "default";
  private static final String DURABLE = "durable";
  private static final String NON_DURABLE = "non-durable";

  private MockServer server;

  @After
  @Override
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if(server !=null) {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testProducerClose(TestContext context) throws Exception {
    doProducerCloseTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testProducerEnd(TestContext context) throws Exception {
    doProducerCloseTestImpl(context, true);
  }

  private void doProducerCloseTestImpl(TestContext context, boolean callEnd) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncReceiveMsg = context.async();
    final Async asyncClose = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the producer is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a receiver link open for the producer
      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          asyncReceiveMsg.complete();
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> {
          serverReceiver.close();
          asyncClose.complete();
        });

        serverReceiver.open();
      });
    });

    // === Client producer handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      // Set up a producer using the client, use it, close it.
      res.createSender(testName).onComplete(context.asyncAssertSuccess(sender -> {
        sender.exceptionHandler(x -> exceptionHandlerCalled.set(true));
        sender.sendWithAck(AmqpMessage.create().withBody(sentContent).build()).onComplete(context.asyncAssertSuccess(x -> {
          if (callEnd) {
            sender.end();
          } else {
            sender.close();
          }

          client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
            asyncShutdown.complete();
          }));
        }));
      }));
    }));

    try {
      asyncReceiveMsg.awaitSuccess();
      asyncClose.awaitSuccess();
      asyncShutdown.awaitSuccess();
      context.assertFalse(exceptionHandlerCalled.get(), "exception handler unexpectedly called");
    } finally {
      server.close();
    }
  }

  @Test(timeout = 20000)
  public void testSenderFlowControlMechanisms(TestContext context) throws Exception {
    final long delay = 250;
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncSendInitialCredit = context.async();
    final Async asyncSendSubsequentCredit = context.async();
    final Async asyncShutdown = context.async();

    // === Server handling ====

    server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the sender is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a receiver link open for the sender
      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Disable auto accept and credit prefetch handling, do it (or not) ourselves
        serverReceiver.setAutoAccept(false);
        serverReceiver.setPrefetch(0);

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          // Only flow subsequent credit after a delay and related checks complete
          vertx.setTimer(delay, x -> {
            asyncSendSubsequentCredit.awaitSuccess();
            serverReceiver.flow(1);
          });
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> serverReceiver.close());
        serverReceiver.open();

        // Only flow initial credit after a delay and initial checks complete
        vertx.setTimer(delay, x -> {
          asyncSendInitialCredit.awaitSuccess();
          serverReceiver.flow(1);
        });
      });
    });

    // === Client producer handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      res.createSender(testName).onComplete(context.asyncAssertSuccess(sender -> {
        context.assertTrue(sender.writeQueueFull(), "expected write queue to be full, we have not yet granted credit");
        sender.drainHandler(x -> {
          context
            .assertTrue(asyncSendInitialCredit.isSucceeded(), "should have been called after initial credit delay");

          context.assertTrue(sender.remainingCredits() > 0);
          context.assertNotNull(sender.unwrap());

          context.assertFalse(sender.writeQueueFull(), "expected write queue not to be full, we just granted credit");

          // Send message using the credit
          sender.send(AmqpMessage.create().withBody(sentContent).build());
          context.assertTrue(sender.writeQueueFull(), "expected write queue to be full, we just used all the credit");

          // Now replace the drain handler, have it act on subsequent credit arriving
          sender.drainHandler(y -> {
            context
              .assertTrue(asyncSendSubsequentCredit.isSucceeded(), "should have been called after 2nd credit delay");
            context.assertFalse(sender.writeQueueFull(), "expected write queue not to be full, we just granted credit");

            client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
              asyncShutdown.complete();
            }));
          });
          // Now allow server to send the subsequent credit
          asyncSendSubsequentCredit.complete();
        });
      }));
      // Now allow to send initial credit. Things will kick off again in the drain handler.
      asyncSendInitialCredit.complete();
    }));

    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testSenderClosedRemotelyCallsExceptionHandler(TestContext context) throws Exception {
    doSenderClosedRemotelyCallsExceptionHandlerTestImpl(context, false);
  }

  @Test(timeout = 20000)
  public void testSenderClosedRemotelyWithErrorCallsExceptionHandler(TestContext context) throws Exception {
    doSenderClosedRemotelyCallsExceptionHandlerTestImpl(context, true);
  }

  private void doSenderClosedRemotelyCallsExceptionHandlerTestImpl(TestContext context,
    boolean closeWithError) throws Exception {
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncShutdown = context.async();
    final Async asyncExceptionHandlerCalled = context.async();

    // === Server handling ====

    server = new MockServer(vertx, serverConnection -> {
      // Expect a connection
      serverConnection.openHandler(serverSender -> {
        // Add a close handler
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      // Expect a session to open, when the sender is created
      serverConnection.sessionOpenHandler(ProtonSession::open);

      // Expect a receiver link open for the sender
      serverConnection.receiverOpenHandler(serverReceiver -> {
        Target remoteTarget = serverReceiver.getRemoteTarget();
        context.assertNotNull(remoteTarget, "target should not be null");
        context.assertEquals(testName, remoteTarget.getAddress(), "expected given address");
        // Naive test-only handling
        serverReceiver.setTarget(remoteTarget.copy());

        // Add the message handler
        serverReceiver.handler((delivery, message) -> {
          Section body = message.getBody();
          context.assertNotNull(body, "received body was null");
          context.assertTrue(body instanceof AmqpValue, "unexpected body section type: " + body.getClass());
          context.assertEquals(sentContent, ((AmqpValue) body).getValue(), "Unexpected message body content");

          if (closeWithError) {
            serverReceiver.setCondition(ProtonHelper.condition(AmqpError.INTERNAL_ERROR, "testing-error"));
          }

          // Now close the link server side
          serverReceiver.close();
        });

        // Add a close handler
        serverReceiver.closeHandler(x -> serverReceiver.close());
        serverReceiver.open();
      });
    });

    // === Client producer handling ====

    AmqpClientOptions options = new AmqpClientOptions()
      .setHost("localhost").setPort(server.actualPort());
    client = AmqpClient.create(vertx, options);
    client.connect().onComplete(context.asyncAssertSuccess(res -> {
      res.createSender(testName).onComplete(context.asyncAssertSuccess(sender -> {
        sender.exceptionHandler(ex -> {
          context.assertNotNull(ex, "expected exception");
          context.assertTrue(ex instanceof Exception, "expected vertx exception");
          if (closeWithError) {
            context.assertNotNull(ex.getCause(), "expected cause");
          } else {
            context.assertNull(ex.getCause(), "expected no cause");
          }
          asyncExceptionHandlerCalled.complete();

          client.close().onComplete(context.asyncAssertSuccess(shutdownRes -> {
            asyncShutdown.complete();
          }));
        });
        sender.send(AmqpMessage.create().withBody(sentContent).build());
      }));
    }));

    asyncExceptionHandlerCalled.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testSenderWithTargetCapability(TestContext context) throws ExecutionException, InterruptedException {
    String targetCapability = "queue";

    Async serverLinkOpenAsync = context.async();
    Async clientLinkOpenAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());

      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());
      serverConnection.receiverOpenHandler(serverReceiver -> {
        serverReceiver.closeHandler(res -> serverReceiver.close());

        // Verify the remote terminus details used were as expected
        context.assertNotNull(serverReceiver.getRemoteTarget(), "target should not be null");
        org.apache.qpid.proton.amqp.messaging.Target remoteTarget =
          (org.apache.qpid.proton.amqp.messaging.Target) serverReceiver.getRemoteTarget();
        context.assertFalse(remoteTarget.getDynamic(), "dynamic target should not be requested");
        context.assertEquals(name.getMethodName(), remoteTarget.getAddress(), "expected target address to be set");

        Symbol[] expectedSourceCapabilities = new Symbol[] { Symbol.valueOf(targetCapability) };
        Symbol[] capabilities = remoteTarget.getCapabilities();
        context.assertTrue(Arrays.equals(expectedSourceCapabilities, capabilities), "Unexpected capabilities: " + Arrays.toString(capabilities));

        // Set the local terminus details
        org.apache.qpid.proton.amqp.messaging.Target target =
          (org.apache.qpid.proton.amqp.messaging.Target) remoteTarget.copy();
        serverReceiver.setTarget(target);

        serverReceiver.open();

        serverLinkOpenAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));

    client.connect().onComplete(context.asyncAssertSuccess(res -> {

      AmqpSenderOptions options = new AmqpSenderOptions().addCapability(targetCapability);

      res.createSender(name.getMethodName(), options).onComplete(context.asyncAssertSuccess(
        sender -> {
          clientLinkOpenAsync.complete();
        }));
    }));

    serverLinkOpenAsync.awaitSuccess();
    clientLinkOpenAsync.awaitSuccess();

  }

  @Test(timeout = 20000)
  public void testDynamicSenderWithOptions(TestContext context) throws ExecutionException, InterruptedException {
    String address = UUID.randomUUID().toString();

    Async serverLinkOpenAsync = context.async();
    Async clientLinkOpenAsync = context.async();

    server = new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(result -> serverConnection.open());

      serverConnection.sessionOpenHandler(ProtonSession::open);
      serverConnection.closeHandler(x -> serverConnection.close());
      serverConnection.receiverOpenHandler(serverReceiver -> {
        serverReceiver.closeHandler(res -> serverReceiver.close());

        // Verify the remote terminus details used were as expected
        context.assertNotNull(serverReceiver.getRemoteTarget(), "target should not be null");
        org.apache.qpid.proton.amqp.messaging.Target remoteTarget =
          (org.apache.qpid.proton.amqp.messaging.Target) serverReceiver.getRemoteTarget();
        context.assertTrue(remoteTarget.getDynamic(), "expected dynamic target to be requested");
        context.assertNull(remoteTarget.getAddress(), "expected no target address to be set");

        // Set the local terminus details
        org.apache.qpid.proton.amqp.messaging.Target target =
          (org.apache.qpid.proton.amqp.messaging.Target) remoteTarget.copy();
        target.setAddress(address);
        serverReceiver.setTarget(target);

        serverReceiver.open();

        serverLinkOpenAsync.complete();
      });
    });

    client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost("localhost").setPort(server.actualPort()));

    client.connect().onComplete(context.asyncAssertSuccess(res -> {

      res.createSender(null,
        new AmqpSenderOptions()
          .setDynamic(true)).onComplete(context.asyncAssertSuccess(
        sender -> {
          context.assertNotNull(sender.address());

          clientLinkOpenAsync.complete();
        }));
    }));

    serverLinkOpenAsync.awaitSuccess();
    clientLinkOpenAsync.awaitSuccess();
  }

  @Test(timeout = 10000)
  public void testAcknowledgementHandling(TestContext context) throws Exception {
    String queue = UUID.randomUUID().toString();
    List<Object> recieved = new CopyOnWriteArrayList<>();
    CountDownLatch acksRecieved = new CountDownLatch(5);

    server = setupMockServerForAckHandling(context, recieved);

    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()));
    client.connect()
      .onComplete(context.asyncAssertSuccess(connection -> {

        connection.createSender(queue).onComplete(context.asyncAssertSuccess(sender -> {

          AmqpMessage msgAccept = AmqpMessage.create().withBody(ACCEPT).build();
          sender.sendWithAck(msgAccept).onComplete(context.asyncAssertSuccess(ack -> {
            acksRecieved.countDown();
          }));

          AmqpMessage msgRelease = AmqpMessage.create().withBody(RELEASE).build();
          sender.sendWithAck(msgRelease).onComplete(context.asyncAssertFailure(err -> {
            context.assertTrue(err.getMessage().contains("RELEASED"));
            acksRecieved.countDown();
          }));

          AmqpMessage msgModifyFailed = AmqpMessage.create().withBody(MODIFY_FAILED).build();
          sender.sendWithAck(msgModifyFailed).onComplete(context.asyncAssertFailure(err -> {
            context.assertTrue(err.getMessage().contains("MODIFIED"));
            acksRecieved.countDown();
          }));

          AmqpMessage msgModifyFailedUH = AmqpMessage.create().withBody(MODIFY_FAILED_U_H).build();
          sender.sendWithAck(msgModifyFailedUH).onComplete(context.asyncAssertFailure(err -> {
            context.assertTrue(err.getMessage().contains("MODIFIED"));
            acksRecieved.countDown();
          }));

          AmqpMessage msgRejected = AmqpMessage.create().withBody(REJECT).build();
          sender.sendWithAck(msgRejected).onComplete(context.asyncAssertFailure(err -> {
            context.assertTrue(err.getMessage().contains("REJECTED"));
            acksRecieved.countDown();
          }));
        }));
      }));

    assertThat(acksRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(recieved).containsExactly(ACCEPT, RELEASE, MODIFY_FAILED, MODIFY_FAILED_U_H, REJECT);
  }

  private MockServer setupMockServerForAckHandling(TestContext context, List<Object> payloads) throws Exception {
    return new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.receiverOpenHandler(serverReceiver-> {
        serverReceiver.setAutoAccept(false);
        serverReceiver.handler((delivery, message) -> {
          Section section = message.getBody();
          context.assertTrue(section instanceof AmqpValue);
          context.assertNotNull(((AmqpValue) section).getValue());
          context.assertTrue(((AmqpValue) section).getValue() instanceof String);

          String payload = (String) ((AmqpValue) section).getValue();

          payloads.add(payload);
            switch(payload) {
            case ACCEPT:
              delivery.disposition(Accepted.getInstance(), true);
              break;
            case RELEASE:
              delivery.disposition(Released.getInstance(), true);
              break;
            case MODIFY_FAILED:
              Modified modifiedFailed = new Modified();
              modifiedFailed.setDeliveryFailed(true);
              delivery.disposition(modifiedFailed, true);
              break;
            case MODIFY_FAILED_U_H:
              Modified modifiedFailedUH = new Modified();
              modifiedFailedUH.setDeliveryFailed(true);
              modifiedFailedUH.setUndeliverableHere(true);
              delivery.disposition(modifiedFailedUH, true);
              break;
            case REJECT:
              delivery.disposition(new Rejected(), true);
              break;
            default:
              context.fail("Unexpected message payload recieved");
            }
        });

        serverReceiver.open();
      });
    });
  }

  @Test(timeout = 10000)
  public void testCreatingSenderWithoutCreatingConnectionFirst(TestContext context) throws Exception {
    String queue = UUID.randomUUID().toString();
    List<Object> recieved = new CopyOnWriteArrayList<>();
    CountDownLatch ackRecieved = new CountDownLatch(1);
    String message = "testCreatingSenderWithoutCreatingConnectionFirst";
    server = setupMockServerForCreatingSenderWithoutConnectionFirst(context, recieved, null);

    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()));
    client.createSender(queue).onComplete(context.asyncAssertSuccess(sender -> {
        AmqpMessage msgAccept = AmqpMessage.create().withBody(message).build();
        sender.sendWithAck(msgAccept).onComplete(context.asyncAssertSuccess(ack -> {
          ackRecieved.countDown();
        }));
      }));

    assertThat(ackRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(recieved).containsExactly(message);
  }

  @Test(timeout = 10000)
  public void testCreatingSenderWithOptionsWithoutCreatingConnectionFirst(TestContext context) throws Exception {
    String senderLinkName = "notUsuallyExplicitlySetForSendersButEasilyVerified";
    String queue = UUID.randomUUID().toString();
    List<Object> recieved = new CopyOnWriteArrayList<>();
    CountDownLatch ackRecieved = new CountDownLatch(1);
    String message = "testCreatingSenderWithOptionsWithoutCreatingConnectionFirst";

    server = setupMockServerForCreatingSenderWithoutConnectionFirst(context, recieved, senderLinkName);

    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()));
    client.createSender(queue, new AmqpSenderOptions().setLinkName(senderLinkName))
      .onComplete(context.asyncAssertSuccess(sender -> {

        AmqpMessage msgAccept = AmqpMessage.create().withBody(message).build();
        sender.sendWithAck(msgAccept).onComplete(context.asyncAssertSuccess(ack -> {
          ackRecieved.countDown();
        }));
      }));

    assertThat(ackRecieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(recieved).containsExactly(message);
  }

  private MockServer setupMockServerForCreatingSenderWithoutConnectionFirst(TestContext context,  List<Object> payloads, String linkName) throws Exception {
    return new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.receiverOpenHandler(serverReceiver-> {
        if(linkName != null) {
          context.assertEquals(linkName, serverReceiver.getName());
        }

        serverReceiver.handler((delivery, message) -> {
          Section section = message.getBody();
          context.assertTrue(section instanceof AmqpValue);
          Object value = ((AmqpValue) section).getValue();
          context.assertNotNull(value);

          payloads.add(value);
        });

        serverReceiver.open();
      });
    });
  }

  @Test(timeout = 10000)
  public void testMessageDurability(TestContext context) throws Exception {
    List<Object> payloads = new CopyOnWriteArrayList<>();
    CountDownLatch recieved = new CountDownLatch(3);

    server = setupMockServerForDurabilityHandling(context, recieved, payloads);

    client = AmqpClient.create(new AmqpClientOptions()
      .setHost("localhost")
      .setPort(server.actualPort()));
    client.connect().onComplete(context.asyncAssertSuccess(connection -> {
        connection.createSender(UUID.randomUUID().toString()).onComplete(context.asyncAssertSuccess(sender -> {

          AmqpMessage msgDefault = AmqpMessage.create().withBody(DEFAULT).build(); //non-durable by omission
          sender.send(msgDefault);

          AmqpMessage msgDurable = AmqpMessage.create().withBody(DURABLE).durable(true).build();
          sender.send(msgDurable);

          AmqpMessage msgNonDurable = AmqpMessage.create().withBody(NON_DURABLE).durable(false).build();
          sender.send(msgNonDurable);
        }));
      }));

    assertThat(recieved.await(6, TimeUnit.SECONDS)).isTrue();
    assertThat(payloads).containsExactly(DEFAULT, DURABLE, NON_DURABLE);
  }

  private MockServer setupMockServerForDurabilityHandling(TestContext context, CountDownLatch recieved, List<Object> payloads) throws Exception {
    return new MockServer(vertx, serverConnection -> {
      serverConnection.openHandler(serverSender -> {
        serverConnection.closeHandler(x -> serverConnection.close());
        serverConnection.open();
      });

      serverConnection.sessionOpenHandler(serverSession -> {
        serverSession.closeHandler(x -> serverSession.close());
        serverSession.open();
      });

      serverConnection.receiverOpenHandler(serverReceiver-> {
        serverReceiver.handler((delivery, message) -> {
          Section section = message.getBody();
          context.assertTrue(section instanceof AmqpValue);
          context.assertNotNull(((AmqpValue) section).getValue());
          context.assertTrue(((AmqpValue) section).getValue() instanceof String);

          String payload = (String) ((AmqpValue) section).getValue();

          switch(payload) {
            case DEFAULT:
              context.assertNull(message.getProperties());
              context.assertFalse(message.isDurable());
              break;
            case DURABLE:
              context.assertTrue(message.isDurable());
              break;
            case NON_DURABLE:
              context.assertFalse(message.isDurable());
              break;
            default:
              context.fail("Unexpected message payload recieved");
          }

          payloads.add(payload);
          recieved.countDown();
        });

        serverReceiver.open();
      });
    });
  }
}
