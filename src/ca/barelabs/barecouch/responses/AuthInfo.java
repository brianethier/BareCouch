package ca.barelabs.barecouch.responses;


public class AuthInfo {

    private String authenticated;
    private String[] authentication_handlers;
    private String authentication_db;

    public String getAuthenticated() {
        return authenticated;
    }

    public void setAuthenticated(String authenticated) {
        this.authenticated = authenticated;
    }

    public String[] getAuthenticationHandlers() {
        return authentication_handlers;
    }

    public void setAuthenticationHandlers(String[] authentication_handlers) {
        this.authentication_handlers = authentication_handlers;
    }

    public String getAuthenticationDb() {
        return authentication_db;
    }

    public void setAuthenticationDb(String authentication_db) {
        this.authentication_db = authentication_db;
    }
}
