package io.vertx.ext.amqp;

import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

@DataObject(generateConverter = true, inheritConverter = true, publicConverter = true)
public class AmqpLinkOptions {

    private String name;
    private boolean dynamicAddress;

    // TODO Not sure they belong here.
    private boolean autoSettle;
    private boolean autoDrained;

    public AmqpLinkOptions() {
        super();
    }

    public AmqpLinkOptions(AmqpLinkOptions other) {
        this.name = other.name;
        this.dynamicAddress = other.dynamicAddress;
        this.autoSettle = other.autoSettle;
        this.autoDrained = other.autoDrained;
    }


    public AmqpLinkOptions(JsonObject json) {
        // TODO converter
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        // TODO converter
        return json;
    }

    public String getName() {
        return name;
    }

    public AmqpLinkOptions setName(String name) {
        this.name = name;
        return this;
    }

    public boolean isDynamicAddress() {
        return dynamicAddress;
    }

    public AmqpLinkOptions setDynamicAddress(boolean dynamicAddress) {
        this.dynamicAddress = dynamicAddress;
        return this;
    }

    public boolean isAutoSettle() {
        return autoSettle;
    }

    public AmqpLinkOptions setAutoSettle(boolean autoSettle) {
        this.autoSettle = autoSettle;
        return this;
    }

    public boolean isAutoDrained() {
        return autoDrained;
    }

    public AmqpLinkOptions setAutoDrained(boolean autoDrained) {
        this.autoDrained = autoDrained;
        return this;
    }
}
