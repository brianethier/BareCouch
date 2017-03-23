package ca.barelabs.barecouch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ca.barelabs.bareconnection.RestResponse;
import com.google.gson.reflect.TypeToken;

public class BulkResult implements Iterable<BulkResult.DocumentResult> {

    private List<DocumentResult> mResults = new ArrayList<>();
    

    public BulkResult(RestResponse response) throws IOException {
    	mResults = response.parseAs(new TypeToken<List<DocumentResult>>(){}.getType());
    }
    
    public List<DocumentResult> getResults() {
        return mResults;
    }

    public int getSize() {
        return mResults.size();
    }

    public Iterator<BulkResult.DocumentResult> iterator() {
        return mResults.iterator();
    }

    public boolean isEmpty() {
        return mResults.isEmpty();
    }

    
    public static class DocumentResult {

    	private String id;
    	private String rev;
    	private String error;
    	private String reason;	
    	
    	public String getId() {
    		return id;
    	}
    	
    	public void setId(String id) {
    		this.id = id;
    	}
    	
    	public String getRev() {
    		return rev;
    	}
    	
    	public void setRev(String rev) {
    		this.rev = rev;
    	}

        public String getError() {
            return error;
        }
        
        public void setError(String error) {
            this.error = error;
        }
        
        public String getReason() {
            return reason;
        }
        
        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
