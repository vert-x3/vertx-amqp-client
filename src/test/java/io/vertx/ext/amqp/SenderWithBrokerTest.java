package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import org.apache.qpid.proton.Proton;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.ApplicationProperties;
import org.apache.qpid.proton.amqp.messaging.Section;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class SenderWithBrokerTest extends ArtemisTestBase {

  //TODO Test the error with bad credentials

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
            done.result().send(AmqpMessage.create().withBody("hello").address(queue).build());
            done.result().send(AmqpMessage.create().withBody("world").address(queue).build());
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
            done.result().sendWithAck(AmqpMessage.create().withBody("hello").address(queue).build(), x -> {
              if (x.succeeded()) {
                acks.incrementAndGet();
                done.result().sendWithAck(AmqpMessage.create().withBody("world").address(queue).build(), y -> {
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

        AmqpMessage message = AmqpMessage.create().withBody(sentContent).applicationProperties(applicationProperties).build();
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
}
