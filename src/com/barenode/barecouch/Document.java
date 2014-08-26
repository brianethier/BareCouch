package com.barenode.barecouch;


public class Document {

    private String _rev;
    private String _id;


    public Document() {
        this(null, null);
    }

    public Document(String id) {
        this(id, null);
    }

    public Document(String id, String rev) {
        _id = id;
        _rev = rev;
    }


    public String getId() {
        return _id;
    }

    public void setId(String id) {
        _id = id;
    }

    public String getRev() {
        return _rev;
    }

    public void setRev(String rev) {
        _rev = rev;
    }
}
