package io.orionmesh.client;

public class ResourceNotFound extends OrionException {
    public final String kind;
    public final String name;

    public ResourceNotFound(String kind, String name) {
        super(kind + "/" + name + " not found");
        this.kind = kind;
        this.name = name;
    }
}
