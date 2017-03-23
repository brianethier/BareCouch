package ca.barelabs.barecouch;

import java.util.List;

public class DocumentBulkRequest {

    private List<?> docs;
    

    public List<?> getDocs() {
        return docs;
    }

    public void setDocs(List<?> docs) {
        this.docs = docs;
    }
}
