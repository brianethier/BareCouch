package ca.barelabs.barecouch.responses;

public class DocumentResponse extends Response {

    private String id;
    private String rev;

    public String getId() {
        return id;
    }

    public String getRev() {
        return rev;
    }
}