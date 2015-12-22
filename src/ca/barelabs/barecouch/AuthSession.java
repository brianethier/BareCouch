package ca.barelabs.barecouch;


public class AuthSession {

    private boolean ok;
    private UserCtx userCtx;
    private AuthInfo info;

    public boolean isOk() {
        return ok;
    }

    public void setOk(boolean ok) {
        this.ok = ok;
    }

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
