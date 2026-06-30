package io.orionmesh.client;

public class ApplyFailed extends OrionException {
    public final int status;
    public final String detail;

    public ApplyFailed(int status, String detail) {
        super("apply failed (" + status + "): " + detail);
        this.status = status;
        this.detail = detail;
    }
}
