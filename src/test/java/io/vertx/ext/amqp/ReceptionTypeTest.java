package io.vertx.ext.amqp;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.Symbol;
import org.apache.qpid.proton.amqp.messaging.AmqpSequence;
import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class ReceptionTypeTest extends ArtemisTestBase {


  private Vertx vertx;
  private AmqpConnection connection;
  private String address;

  @Before
  public void init() {
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
  public void testNull() {
    List<Boolean> list = new CopyOnWriteArrayList<>();
    connection.receiver(address, message -> {
      list.add(message.isBodyNull());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          // Send Amqpvalue(null)
          usage.produce(address, 1, null, () -> new AmqpValue(null));
          // Send no body
          usage.produce(address, 1, null, () -> null);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(true, true);
  }

  @Test
  public void testBoolean() {
    List<Boolean> list = new CopyOnWriteArrayList<>();
    connection.receiver(address, message -> {
      list.add(message.getBodyAsBoolean());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> true);
          usage.produce(address, 1, null, () -> false);
          usage.produce(address, 1, null, () -> Boolean.TRUE);
          usage.produce(address, 1, null, () -> Boolean.FALSE);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 4);
    assertThat(list).containsExactly(true, false, true, false);
  }

  @Test
  public void testByte() {
    List<Object> list = new CopyOnWriteArrayList<>();
    byte b = 1;
    connection.receiver(address, message -> {
      list.add(message.getBodyAsByte());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> b);
          usage.produce(address, 1, null, () -> Byte.valueOf(b));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(b, b);
  }

  @Test
  public void testShort() {
    List<Object> list = new CopyOnWriteArrayList<>();
    short s = 2;
    connection.receiver(address, message -> {
      list.add(message.getBodyAsShort());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> s);
          usage.produce(address, 1, null, () -> Short.valueOf(s));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(s, s);
  }

  @Test
  public void testInteger() {
    List<Object> list = new CopyOnWriteArrayList<>();
    int i = 3;
    connection.receiver(address, message -> {
      list.add(message.getBodyAsInteger());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> i);
          usage.produce(address, 1, null, () -> Integer.valueOf(i));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(i, i);
  }

  @Test
  public void testLong() {
    List<Object> list = new CopyOnWriteArrayList<>();
    long l = Long.MAX_VALUE - 1;
    connection.receiver(address, message -> {
      list.add(message.getBodyAsLong());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> l);
          usage.produce(address, 1, null, () -> Long.valueOf(l));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(l, l);
  }

  @Test
  public void testFloat() {
    List<Object> list = new CopyOnWriteArrayList<>();
    float f = 12.34f;
    connection.receiver(address, message -> {
      list.add(message.getBodyAsFloat());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> f);
          usage.produce(address, 1, null, () -> Float.valueOf(f));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(f, f);
  }

  @Test
  public void testDouble() {
    List<Object> list = new CopyOnWriteArrayList<>();
    double d = 56.78;
    connection.receiver(address, message -> {
      list.add(message.getBodyAsDouble());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> d);
          usage.produce(address, 1, null, () -> Double.valueOf(d));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(d, d);
  }

  @Test
  public void testCharacter() {
    List<Object> list = new CopyOnWriteArrayList<>();
    char c = 'c';
    connection.receiver(address, message -> {
      list.add(message.getBodyAsChar());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> c);
          usage.produce(address, 1, null, () -> Character.valueOf(c));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 2);
    assertThat(list).containsExactly(c, c);
  }

  @Test
  public void testTimestamp() {
    List<Object> list = new CopyOnWriteArrayList<>();
    Instant instant = Instant.now();
    connection.receiver(address, message -> {
      list.add(message.getBodyAsTimestamp());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> Date.from(instant));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
  }

  @Test
  public void testUUID() {
    List<Object> list = new CopyOnWriteArrayList<>();
    UUID uuid = UUID.randomUUID();
    connection.receiver(address, message -> {
      list.add(message.getBodyAsUUID());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> uuid);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(uuid);
  }

  @Test
  public void testBinary() {
    List<Object> list = new CopyOnWriteArrayList<>();
    Buffer buffer = Buffer.buffer("hello !!!");
    connection.receiver(address, message -> {
      list.add(message.getBodyAsBinary());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> new Data(new Binary(buffer.getBytes())));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(buffer);
  }

  @Test
  public void testString() {
    List<Object> list = new CopyOnWriteArrayList<>();
    String string = "hello !";
    connection.receiver(address, message -> {
      list.add(message.getBodyAsString());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> string);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(string);
  }

  @Test
  public void testSymbol() {
    List<Object> list = new CopyOnWriteArrayList<>();
    String string = "my-symbol";
    connection.receiver(address, message -> {
      list.add(message.getBodyAsSymbol());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> Symbol.getSymbol("my-symbol"));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(string);
  }

  @Test
  public void testListPassedAsAmqpSequence() {
    List<Object> list = new CopyOnWriteArrayList<>();
    List<Object> l = new ArrayList<>();
    l.add("foo");
    l.add(1);
    l.add(true);
    connection.receiver(address, message -> {
      list.add(message.getBodyAsList());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> new AmqpSequence(l));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(l);
  }

  @Test
  public void testListPassedAsAmqpValue() {
    List<Object> list = new CopyOnWriteArrayList<>();
    List<Object> l = new ArrayList<>();
    l.add("foo");
    l.add(1);
    l.add(true);
    connection.receiver(address, message -> {
      list.add(message.getBodyAsList());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> new AmqpValue(l));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(l);
  }


  @Test
  public void testMap() {
    List<Map<String, String>> list = new CopyOnWriteArrayList<>();
    Map<String, String> map = new HashMap<>();
    map.put("1", "hello");
    map.put("2", "bonjour");
    connection.receiver(address, message -> {
      list.add(message.getBodyAsMap());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> new AmqpValue(map));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list.get(0)).containsAllEntriesOf(map);
  }


  @Test
  public void testJsonObject() {
    List<Object> list = new CopyOnWriteArrayList<>();
    JsonObject json = new JsonObject().put("data", "message").put("number", 1)
      .put("array", new JsonArray().add(1).add(2).add(3));
    connection.receiver(address, message -> {
      list.add(message.getBodyAsJsonObject());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> new Data(new Binary(json.toBuffer().getBytes())));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(json);
  }

  @Test
  public void testJsonArray() {
    List<Object> list = new CopyOnWriteArrayList<>();
    JsonArray array = new JsonArray().add(1).add(2).add(3);
    connection.receiver(address, message -> {
      list.add(message.getBodyAsJsonArray());
    }, done -> {
      if (done.failed()) {
        done.cause().printStackTrace();
      }
      CompletableFuture.runAsync(() -> {
        try {
          usage.produce(address, 1, null, () -> new Data(new Binary(array.toBuffer().getBytes())));
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    });
    await().until(() -> list.size() == 1);
    assertThat(list).containsExactly(array);
  }
}
