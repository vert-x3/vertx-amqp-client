package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class SenderTypeTest extends ArtemisTestBase {


  private Vertx vertx;
  private AmqpConnection connection;
  private String address;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    AtomicReference<AmqpConnection> reference = new AtomicReference<>();
    client = AmqpClient.create(vertx, new AmqpClientOptions()
      .setHost(host)
      .setPort(port)
      .setUsername(username)
      .setPassword(password))
      .connect(connection -> {
        reference.set(connection.result());
        if (connection.failed()) {
          connection.cause().printStackTrace();
        }
      });

    await().untilAtomic(reference, is(notNullValue()));
    this.connection = reference.get();
    this.address = UUID.randomUUID().toString();
  }

  @After
  public void tearDown() throws InterruptedException {
    super.tearDown();
    vertx.close();
  }

  @Test
  public void testBoolean() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withBooleanAsBody(true).build());
        sender.send(AmqpMessage.create().withBooleanAsBody(false).build());
        sender.send(AmqpMessage.create().withBooleanAsBody(Boolean.TRUE).build());
        sender.send(AmqpMessage.create().withBooleanAsBody(Boolean.FALSE).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 4);

    assertThat(list.stream().map(AmqpMessage::bodyAsBoolean).collect(Collectors.toList()))
      .containsExactly(true, false, true, false);
  }

  @Test
  public void testByte() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    byte value = 1;

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withByteAsBody(value).build());
        sender.send(AmqpMessage.create().withByteAsBody(Byte.valueOf(value)).build());
      }
    });

    await().until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsByte).collect(Collectors.toList()))
      .containsExactly(value, value);
  }

  @Test
  public void testShort() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    short value = 2;

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withShortAsBody(value).build());
        sender.send(AmqpMessage.create().withShortAsBody(Short.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsShort).collect(Collectors.toList()))
      .containsExactly(value, value);
  }


  @Test
  public void testInteger() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    int value = 3;

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withIntegerAsBody(value).build());
        sender.send(AmqpMessage.create().withIntegerAsBody(Integer.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsInteger).collect(Collectors.toList()))
      .containsExactly(value, value);
  }

  @Test
  public void testLong() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    long value = 25;

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withLongAsBody(value).build());
        sender.send(AmqpMessage.create().withLongAsBody(Long.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsLong).collect(Collectors.toList()))
      .containsExactly(value, value);
  }

  @Test
  public void testFloat() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    float value = 23.45f;

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withFloatAsBody(value).build());
        sender.send(AmqpMessage.create().withFloatAsBody(Float.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsFloat).collect(Collectors.toList()))
      .containsExactly(value, value);
  }

  @Test
  public void testDouble() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    double value = 123.45;

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withDoubleAsBody(value).build());
        sender.send(AmqpMessage.create().withDoubleAsBody(Double.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsDouble).collect(Collectors.toList()))
      .containsExactly(value, value);
  }

  @Test
  public void testCharacter() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    char value = 'a';

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withCharAsBody(value).build());
        sender.send(AmqpMessage.create().withCharAsBody(Character.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::bodyAsChar).collect(Collectors.toList()))
      .containsExactly(value, value);
  }

  @Test
  public void testTimestamp() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    Instant value = Instant.now();

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withInstantAsBody(value).build());
      }
    });

    await().until(() -> list.size() == 1);
  }

  @Test
  public void testUUID() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    UUID value = UUID.randomUUID();

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withUuidAsBody(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsUUID).collect(Collectors.toList()))
      .containsExactly(value);
  }

  @Test
  public void testBinary() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    Buffer value = Buffer.buffer("this is a message");

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);

    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withBufferAsBody(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsBinary).collect(Collectors.toList()))
      .containsExactly(value);
  }

  @Test
  public void testString() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    String value = "this is a message";

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withBody(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsString).collect(Collectors.toList()))
      .containsExactly(value);
  }

  @Test
  public void testSymbol() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    String value = "Newton";

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withSymbolAsBody(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsSymbol).collect(Collectors.toList()))
      .containsExactly(value);
  }

  @Test
  public void testList() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    List<Object> value = new ArrayList<>();
    value.add("foo");
    value.add(1);
    value.add(true);

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withListAsBody(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsList).collect(Collectors.toList()))
      .containsExactly(value);
  }

  @Test
  public void testMap() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    Map<String, String> value = new HashMap<>();
    value.put("1", "foo");
    value.put("2", "bar");
    value.put("3", "baz");

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withMapAsBody(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> list.size() == 1);

    assertThat(list.get(0).bodyAsMap()).containsAllEntriesOf(value);
  }

  @Test
  public void testJsonObject() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    JsonObject value = new JsonObject().put("data", "message").put("number", 1)
      .put("array", new JsonArray().add(1).add(2).add(3));

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withJsonObjectAsBody(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsJsonObject).collect(Collectors.toList()))
      .containsExactly(value);
  }

  @Test
  public void testJsonArray() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();
    JsonArray value = new JsonArray().add(1).add(2).add(3);

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().withJsonArrayAsBody(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::bodyAsJsonArray).collect(Collectors.toList()))
      .containsExactly(value);
  }
}
