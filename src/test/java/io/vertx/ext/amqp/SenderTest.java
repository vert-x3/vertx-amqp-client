package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSession;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.amqp.transport.Target;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class SenderTest extends ArtemisTestBase {

  //TODO Test the error with bad credentials

  private Vertx vertx;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    vertx = Vertx.vertx();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    if (vertx != null) {
      vertx.close();
    }
  }

  @Test
  public void testThatMessagedAreSent() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    usage.consumeStrings(queue, 2, 1, TimeUnit.MINUTES, null, list::add);
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().sender(queue, done -> {
          if (done.failed()) {
            done.cause().printStackTrace();
          } else {
            // Sending a few messages
            done.result().send(AmqpMessage.create().body("hello").address(queue).build());
            done.result().send(AmqpMessage.create().body("world").address(queue).build());
          }
        });
      }
    );

    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }

  @Test
  public void testThatMessagedAreAcknowledged() {
    String queue = UUID.randomUUID().toString();
    List<String> list = new CopyOnWriteArrayList<>();
    AtomicInteger acks = new AtomicInteger();
    usage.consumeStrings(queue, 2, 1, TimeUnit.MINUTES, null, list::add);
    client = AmqpClient.create(new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password)
    ).connect(connection -> {
        connection.result().sender(queue, done -> {
          if (done.failed()) {
            done.cause().printStackTrace();
          } else {
            // Sending a few messages
            done.result().sendWithAck(AmqpMessage.create().body("hello").address(queue).build(), x -> {
              if (x.succeeded()) {
                acks.incrementAndGet();
                done.result().sendWithAck(AmqpMessage.create().body("world").address(queue).build(), y -> {
                  if (y.succeeded()) {
                    acks.incrementAndGet();
                  }
                });
              }
            });
          }
        });
      }
    );

    await().until(() -> acks.get() == 2);
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly("hello", "world");
  }

  @Test(timeout = 20000)
  public void testSendBasicMessage(TestContext context) {
    String testName = name.getMethodName();
    String sentContent = "myMessageContent-" + testName;
    String propKey = "appPropKey";
    String propValue = "appPropValue";

    Async asyncRecvMsg = context.async();


    AmqpClient client = AmqpClient.create(vertx,
      new AmqpClientOptions().setHost(host).setPort(port).setUsername(username).setPassword(password));
    client.connect(res -> {
      context.assertTrue(res.succeeded());

      res.result().sender(testName, sender -> {
        context.assertTrue(sender.succeeded());

        JsonObject applicationProperties = new JsonObject();
        applicationProperties.put(propKey, propValue);

        AmqpMessage message = AmqpMessage.create().body(sentContent).applicationProperties(applicationProperties).build();
        sender.result().send(message);
        context.assertEquals(testName, sender.result().address(), "address was not as expected");
      });
    });

    ProtonClient proton = ProtonClient.create(vertx);
    proton.connect(host, port, username, password, res -> {
      context.assertTrue(res.succeeded());

      org.apache.qpid.proton.message.Message protonMsg = Proton.message();
      protonMsg.setBody(new AmqpValue(sentContent));

      ProtonConnection conn = res.result().open();

      ProtonReceiver receiver = conn.createReceiver(testName);
      receiver.handler((d, m) -> {
        Section body = m.getBody();
        context.assertNotNull(body);
        context.assertTrue(body instanceof AmqpValue);
        Object actual = ((AmqpValue) body).getValue();

        context.assertEquals(sentContent, actual, "Unexpected message body");

        ApplicationProperties applicationProperties = m.getApplicationProperties();
        context.assertNotNull(applicationProperties, "application properties section not present");
        context.assertTrue(applicationProperties.getValue().containsKey(propKey), "property key not present");
        context.assertEquals(propValue, applicationProperties.getValue().get(propKey), "Unexpected property value");

        client.close(x -> asyncRecvMsg.complete());
        conn.closeHandler(closeResult -> conn.disconnect()).close();
      }).open();
    });

    asyncRecvMsg.awaitSuccess();

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
    artemis.stop();

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

    // === Bridge producer handling ====

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost").setPort(server.actualPort()).setUsername(username).setUsername(password);
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      // Set up a producer using the bridge, use it, close it.
      context.assertTrue(res.succeeded());

      res.result().sender(testName, done -> {
        AmqpSender sender = done.result();
        sender.exceptionHandler(x -> exceptionHandlerCalled.set(true));
        sender.sendWithAck(AmqpMessage.create().body(sentContent).build(), x -> {
          context.assertTrue(x.succeeded());

          if (callEnd) {
            sender.end();
          } else {
            sender.close(null);
          }

          client.close(shutdownRes -> {
            context.assertTrue(shutdownRes.succeeded());
            asyncShutdown.complete();
          });
        });
      });
    });

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
    artemis.stop();

    final long delay = 250;
    final String testName = name.getMethodName();
    final String sentContent = "myMessageContent-" + testName;

    final Async asyncSendInitialCredit = context.async();
    final Async asyncSendSubsequentCredit = context.async();
    final Async asyncShutdown = context.async();

    // === Server handling ====

    MockServer server = new MockServer(vertx, serverConnection -> {
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

    AmqpClientOptions options = new AmqpClientOptions().setReplyEnabled(false)
      .setHost("localhost")
      .setPort(server.actualPort());
    AmqpClient client = AmqpClient.create(vertx, options);
    client.connect(res -> {
      context.assertTrue(res.succeeded());
      res.result().sender(testName, done -> {
        context.assertTrue(done.succeeded());
        AmqpSender sender = done.result();
        context.assertTrue(sender.writeQueueFull(), "expected write queue to be full, we have not yet granted credit");
        sender.drainHandler(x -> {
          context.assertTrue(asyncSendInitialCredit.isSucceeded(), "should have been called after initial credit delay");
          context.assertFalse(sender.writeQueueFull(), "expected write queue not to be full, we just granted credit");

          // Send message using the credit
          sender.send(AmqpMessage.create().body(sentContent).build());
          context.assertTrue(sender.writeQueueFull(), "expected write queue to be full, we just used all the credit");

          // Now replace the drain handler, have it act on subsequent credit arriving
          sender.drainHandler(y -> {
            context.assertTrue(asyncSendSubsequentCredit.isSucceeded(), "should have been called after 2nd credit delay");
            context.assertFalse(sender.writeQueueFull(), "expected write queue not to be full, we just granted credit");

            client.close(shutdownRes -> {
              context.assertTrue(shutdownRes.succeeded());
              asyncShutdown.complete();
            });
          });
          // Now allow server to send the subsequent credit
          asyncSendSubsequentCredit.complete();
        });
      });
      // Now allow to send initial credit. Things will kick off again in the drain handler.
      asyncSendInitialCredit.complete();
    });

    try {
      asyncShutdown.awaitSuccess();
    } finally {
      server.close();
    }
  }
}
