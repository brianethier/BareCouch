package ca.barelabs.barecouch.responses;


public class SessionResponse extends Response {

    private UserCtx userCtx;
    private AuthInfo info;

    public UserCtx getUserCtx() {
        return userCtx;
    }

    public void setUserCtx(UserCtx userCtx) {
        this.userCtx = userCtx;
    }

    public AuthInfo getInfo() {
        return info;
    }

    public void setInfo(AuthInfo info) {
        this.info = info;
    }
}
