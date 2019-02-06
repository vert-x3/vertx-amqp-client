package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.qpid.proton.amqp.*;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
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
    AmqpClient.create(vertx, new AmqpClientOptions()
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
  public void tearDown() {
    vertx.close();
  }

  @Test
  public void testBoolean() {
    List<AmqpMessage> list = new CopyOnWriteArrayList<>();

    usage.consumeMessages(address, 4, 10, TimeUnit.SECONDS, null, list::add);
    connection.sender(address, done -> {
      System.out.println("Got sender");
      if (done.failed()) {
        done.cause().printStackTrace();
      } else {
        AmqpSender sender = done.result();
        sender.send(AmqpMessage.create().body(true).build());
        sender.send(AmqpMessage.create().body(false).build());
        sender.send(AmqpMessage.create().body(Boolean.TRUE).build());
        sender.send(AmqpMessage.create().body(Boolean.FALSE).build());
        System.out.println("Sent...");
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
      return list.size() == 4;
    });

    assertThat(list.stream().map(AmqpMessage::getBodyAsBoolean).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Byte.valueOf(value)).build());
      }
    });

    await().until(() -> list.size() == 2);

    assertThat(list.stream().map(AmqpMessage::getBodyAsByte).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Short.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 2;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsShort).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Integer.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 2;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsInteger).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Long.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 2;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsLong).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Float.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 2;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsFloat).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Double.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 2;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsDouble).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
        sender.send(AmqpMessage.create().body(Character.valueOf(value)).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 2;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsChar).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::getBodyAsUUID).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::getBodyAsBinary).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 1;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsString).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().bodyAsSymbol(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 1;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsSymbol).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 1;
      });

    assertThat(list.stream().map(AmqpMessage::getBodyAsList).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await()
      .pollInterval(2, TimeUnit.SECONDS)
      .until(() -> {
        System.out.println(list);
        return list.size() == 1;
      });

    assertThat(list.get(0).getBodyAsMap()).containsAllEntriesOf(value);
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::getBodyAsJsonObject).collect(Collectors.toList()))
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
        sender.send(AmqpMessage.create().body(value).build());
      }
    });

    await().until(() -> list.size() == 1);

    assertThat(list.stream().map(AmqpMessage::getBodyAsJsonArray).collect(Collectors.toList()))
      .containsExactly(value);
  }
}
