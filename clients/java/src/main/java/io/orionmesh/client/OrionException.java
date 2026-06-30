package io.orionmesh.client;

/** Base class for every checked exception this client raises. */
public class OrionException extends RuntimeException {
    public OrionException(String message) { super(message); }
    public OrionException(String message, Throwable cause) { super(message, cause); }
}
