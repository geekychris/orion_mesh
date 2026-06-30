package io.orionmesh.client;

public class QueueNotFound extends OrionException {
    public final String name;

    public QueueNotFound(String name) {
        super("queue " + name + " not declared — apply a Queue resource first");
        this.name = name;
    }
}
