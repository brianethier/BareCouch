package ca.barelabs.barecouch.responses;


public class UserCtx {

    private String[] roles;
    private String name;

    public String[] getRoles() {
        return roles;
    }

    public void setRoles(String[] roles) {
        this.roles = roles;
    }

    public String getName() {
        return name;
    }

    public void setName (String name) {
        this.name = name;
    }
}
