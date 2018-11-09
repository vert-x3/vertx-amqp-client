package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.CacheReturn;
import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.StreamBase;

@VertxGen
public interface AmqpReceiver extends ReadStream<AmqpMessage>, StreamBase {

    @Fluent
    AmqpReceiver handler(Handler<AmqpMessage> message);

    @Fluent
    AmqpReceiver endHandler(Handler<Void> endHandler);

    @CacheReturn
    String address();

    @Fluent
    AmqpReceiver close();

    @Fluent
    AmqpReceiver close(Handler<AsyncResult<Void>> completionHandler);

    @Fluent
    AmqpReceiver pause();

    @Fluent
    AmqpReceiver resume();

    @Fluent
    AmqpReceiver setMaxBufferedMessages(int maxBufferedMessages);

    @Fluent
    AmqpReceiver exceptionHandler(Handler<Throwable> handler);

    int getMaxBufferedMessages();

    @Override
    @Fluent
    AmqpReceiver fetch(long l);
}
