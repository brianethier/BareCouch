package ca.barelabs.barecouch.responses;


public class Response {

    private String id;
    private String rev;
    private boolean ok;


    public String getId() {
        return id;
    }

    public String getRev() {
        return rev;
    }

    public boolean isOk() {
        return ok;
    }

}