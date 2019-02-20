package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.impl.AmqpMessageBuilderImpl;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@VertxGen
public interface AmqpMessageBuilder {

  static AmqpMessageBuilder create() {
    return new AmqpMessageBuilderImpl();
  }

  AmqpMessage build();

  AmqpMessageBuilder priority(short priority);

  AmqpMessageBuilder id(String id);

  AmqpMessageBuilder address(String address);

  AmqpMessageBuilder replyTo(String replyTo);

  AmqpMessageBuilder correlationId(String correlationId);

  AmqpMessageBuilder withBody(String value);

  AmqpMessageBuilder withSymbolAsBody(String value);

  AmqpMessageBuilder subject(String subject);

  AmqpMessageBuilder contentType(String ct);

  AmqpMessageBuilder contentEncoding(String ct);

  AmqpMessageBuilder expiryTime(long expiry);

  AmqpMessageBuilder creationTime(long ct);

  AmqpMessageBuilder ttl(long ttl);

  AmqpMessageBuilder groupId(String gi);

  AmqpMessageBuilder replyToGroupId(String rt);

  AmqpMessageBuilder applicationProperties(JsonObject props);

  AmqpMessageBuilder withBooleanAsBody(boolean v);

  AmqpMessageBuilder withByteAsBody(byte v);

  AmqpMessageBuilder withShortAsBody(short v);

  AmqpMessageBuilder withIntegerAsBody(int v);

  AmqpMessageBuilder withLongAsBody(long v);

  AmqpMessageBuilder withFloatAsBody(float v);

  AmqpMessageBuilder withDoubleAsBody(double v);

  AmqpMessageBuilder withCharAsBody(char c);

  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  AmqpMessageBuilder withInstantAsBody(Instant v);

  @GenIgnore(GenIgnore.PERMITTED_TYPE)
  AmqpMessageBuilder withUuidAsBody(UUID v);

  @GenIgnore
  AmqpMessageBuilder withListAsBody(List list);

  @GenIgnore
  AmqpMessageBuilder withMapAsBody(Map map);

  AmqpMessageBuilder withBufferAsBody(Buffer buffer);

  AmqpMessageBuilder withJsonObjectAsBody(JsonObject json);

  AmqpMessageBuilder withJsonArrayAsBody(JsonArray json);
}
