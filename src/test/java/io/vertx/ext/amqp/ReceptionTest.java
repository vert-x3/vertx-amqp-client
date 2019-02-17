package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.Accepted;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Source;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@RunWith(VertxUnitRunner.class)
public class ReceptionTest extends ArtemisTestBase {

  private Vertx vertx;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test(timeout = 20000)
  public void testReceiveBasicMessage(TestContext context) {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;
    String propKey = "appPropKey";
    String propValue = "appPropValue";

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    AmqpClient client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost(host).setPort(port).setPassword(password).setUsername(username));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName, msg -> {
        context.assertNotNull(msg, "message was null");
        context.assertNotNull(msg.getBodyAsString(), "amqp message body content was null");
        context.assertEquals(sentContent, msg.getBodyAsString(), "amqp message body was not as expected");

        // Check the application property was present
        context.assertTrue(msg.getApplicationProperties() != null, "application properties element not present");
        JsonObject appProps = msg.getApplicationProperties();
        context.assertTrue(appProps.containsKey(propKey), "expected property key element not present");
        context.assertEquals(propValue, appProps.getValue(propKey), "app property value not as expected");
        client.close(x -> asyncShutdown.complete());
      }, done -> {
        context.assertEquals(testName, done.result().address(), "address was not as expected");

        ProtonClient proton = ProtonClient.create(vertx);
        proton.connect(host, port, username, password, res2 -> {
          context.assertTrue(res2.succeeded());
          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));
          Map<String, Object> props = new HashMap<>();
          props.put(propKey, propValue);
          ApplicationProperties appProps = new ApplicationProperties(props);
          protonMsg.setApplicationProperties(appProps);
          ProtonConnection conn = res2.result().open();

          ProtonSender sender = conn.createSender(testName).open();
          sender.send(protonMsg, delivery -> {
            context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
            context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
            context.assertTrue(delivery.remotelySettled(), "message was not settled");
            conn.closeHandler(closeResult -> conn.disconnect()).close();
            asyncSendMsg.complete();
          });
        });
      });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReceiveBasicMessageAsStream(TestContext context) {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host).setPort(port).setUsername(username).setPassword(password));
    client.connect(res -> {
      // Set up a read stream using the bridge
      res.result().receiver(testName, established -> {
        established.result().handler(msg -> {
          context.assertNotNull(msg, "message was null");

          String content = msg.getBodyAsString();
          context.assertNotNull(content, "amqp message body content was null");

          context.assertEquals(sentContent, content, "amqp message body was not as expected");

          client.close(shutdownRes -> {
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });

        // Send it a message from a regular AMQP client
        ProtonClient proton = ProtonClient.create(vertx);
        proton.connect(host, port, username, password, res2 -> {
          context.assertTrue(res2.succeeded());

          org.apache.qpid.proton.message.Message protonMsg = Proton.message();
          protonMsg.setBody(new AmqpValue(sentContent));

          ProtonConnection conn = res2.result().open();

          ProtonSender sender = conn.createSender(testName).open();
          sender.send(protonMsg, delivery -> {
            context.assertNotNull(delivery.getRemoteState(), "message had no remote state");
            context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message was not accepted");
            context.assertTrue(delivery.remotelySettled(), "message was not settled");

            conn.closeHandler(closeResult -> conn.disconnect()).close();

            asyncSendMsg.complete();
          });

        });

      });

    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testReceiveMultipleMessageAfterDelayedHandlerAddition(TestContext context) {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    int msgCount = 5;

    AmqpClient client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host).setPort(port).setPassword(password).setUsername(username));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      // Set up a consumer using the bridge but DONT register the handler
      res.result().receiver(testName, done -> {
        context.assertTrue(done.succeeded());

        // Send some message from a regular AMQP client
        sendAFewMessages(context, testName, sentContent, asyncSendMsg, msgCount);

        // Add the handler after a delay
        vertx.setTimer(500, x -> {
          AtomicInteger received = new AtomicInteger();
          done.result().handler(msg -> {
            int msgNum = received.incrementAndGet();
            String content = msg.getBodyAsString();
            context.assertNotNull(content, "amqp message " + msgNum + " body content was null");
            context.assertEquals(sentContent, content, "amqp message " + msgNum + " body not as expected");

            if (msgNum == msgCount) {
              client.close(shutdownRes -> {
                context.assertTrue(shutdownRes.succeeded());
                asyncShutdown.complete();
              });
            }
          });
        }); // timer
      }); // receiver
    }); // connect
    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  private void sendAFewMessages(TestContext context, String testName, String sentContent, Async asyncSendMsg, int msgCount) {
    ProtonClient proton = ProtonClient.create(vertx);
    proton.connect(host, port, username, password, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();
      ProtonSender sender = conn.createSender(testName).open();
      for (int i = 1; i <= msgCount; i++) {
        final int msgNum = i;
        sender.send(protonMsg, delivery -> {
          context.assertNotNull(delivery.getRemoteState(), "message " + msgNum + " had no remote state");
          context.assertTrue(delivery.getRemoteState() instanceof Accepted, "message " + msgNum + " was not accepted");
          context.assertTrue(delivery.remotelySettled(), "message " + msgNum + " was not settled");

          if (msgNum == msgCount) {
            conn.closeHandler(closeResult -> conn.disconnect()).close();
            asyncSendMsg.complete();
          }
        });
      }
    });
  }

  @Test(timeout = 20000)
  public void testReceiveMultipleMessageAfterPause(TestContext context) {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;

    Async asyncShutdown = context.async();
    Async asyncSendMsg = context.async();

    final int pauseCount = 2;
    final int totalMsgCount = 5;
    final int delay = 500;

    AmqpClient client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost(host).setPort(port).setUsername(username).setPassword(password));
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      final AtomicInteger received = new AtomicInteger();
      final AtomicLong pauseStartTime = new AtomicLong();
      final AtomicReference<AmqpReceiver> receiver = new AtomicReference<>();
      // Set up a consumer using the bridge
      res.result().receiver(testName,
        msg -> {
          int msgNum = received.incrementAndGet();
          String amqpBodyContent = msg.getBodyAsString();
          context.assertNotNull(amqpBodyContent, "message " + msgNum + " jsonObject body was null");
          context.assertNotNull(amqpBodyContent, "amqp message " + msgNum + " body content was null");
          context.assertEquals(sentContent, amqpBodyContent, "amqp message " + msgNum + " body not as expected");

          // Pause once we get initial messages
          if (msgNum == pauseCount) {
            receiver.get().pause();
            // Resume after a delay
            pauseStartTime.set(System.currentTimeMillis());
            vertx.setTimer(delay, x -> receiver.get().resume());
          }

          // Verify subsequent deliveries occur after the expected delay
          if (msgNum > pauseCount) {
            context.assertTrue(pauseStartTime.get() > 0, "pause start not initialised before receiving msg" + msgNum);
            context.assertTrue(System.currentTimeMillis() + delay > pauseStartTime.get(),
              "delivery occurred before expected");
          }

          if (msgNum == totalMsgCount) {
            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          }
        },
        done -> {
          context.assertTrue(done.succeeded());
          receiver.set(done.result());

          // Send some message from a regular AMQP client
          sendAFewMessages(context, testName, sentContent, asyncSendMsg, totalMsgCount);
        });
    });

    asyncSendMsg.awaitSuccess();
    asyncShutdown.awaitSuccess();
  }

  @Test(timeout = 20000)
  public void testConsumerCloseCompletionNotification(TestContext context) throws Exception {
    artemis.stop();

    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncUnregister = context.async();
    final Async asyncShutdown = context.async();

    final AtomicBoolean exceptionHandlerCalled = new AtomicBoolean();

    MockServer server = new MockServer(vertx,
      serverConnection -> handleReceiverOpenSendMessageThenClose(serverConnection, testName, sentContent, context));

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost").setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().receiver(testName, done -> {
        context.assertTrue(done.succeeded());
        AmqpReceiver receiver = done.result();
        receiver.exceptionHandler(x -> exceptionHandlerCalled.set(true));
        receiver.handler(msg -> {
          String amqpBodyContent = msg.getBodyAsString();
          context.assertNotNull(amqpBodyContent, "amqp message body content was null");
          context.assertEquals(sentContent, amqpBodyContent, "amqp message body not as expected");

          receiver.close(x -> {
            context.assertTrue(x.succeeded(), "Expected close to succeed");
            asyncUnregister.complete();

            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
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
}
