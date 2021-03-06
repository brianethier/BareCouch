package ca.barelabs.barecouch;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ca.barelabs.bareconnection.GsonParser;
import ca.barelabs.bareconnection.IOUtils;
import ca.barelabs.bareconnection.ObjectParser;
import ca.barelabs.bareconnection.RestResponse;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class ChangesResult implements Iterable<ChangesResult.DocumentChange> {

    public static final String FIELD_RESULTS = "results";
    public static final String FIELD_LAST_SEQ = "last_seq";

    private final ChangesQuery mQuery;
    private final ObjectParser mParser;
    private List<DocumentChange> mChanges = new ArrayList<>();
    private String mLastSeq;
    

    public ChangesResult(ChangesQuery query, RestResponse response) throws IOException {
    	mQuery = query;
        mParser = response.getParser();
        parseMetadata(response.parse());
    }

    public ChangesQuery getQuery() {
		return mQuery;
	}

	public ObjectParser getParser() {
		return mParser;
	}

	public List<DocumentChange> getChanges() {
        return mChanges;
    }

    public int getSize() {
        return mChanges.size();
    }

    public Iterator<ChangesResult.DocumentChange> iterator() {
        return mChanges.iterator();
    }

    public boolean isEmpty() {
        return mChanges.isEmpty();
    }

    public String getLastSeq() {
        return mLastSeq;
    }
    
    private final void parseMetadata(String result) {
        JsonReader jsonReader = new JsonReader(new StringReader(result));
        JsonParser jsonParser = new JsonParser();
        try {
            jsonReader.beginObject();
            while (jsonReader.hasNext()) {
                String name = jsonReader.nextName();
                if (name.equals(FIELD_RESULTS)) {
                    jsonReader.beginArray();
                    while(jsonReader.hasNext()) {
                        JsonElement jsonElement = jsonParser.parse(jsonReader);
                        mChanges.add(new DocumentChange(mParser, jsonElement.getAsJsonObject()));
                    }
                    jsonReader.endArray();
                } else if (name.equals(FIELD_LAST_SEQ)) {
                	mLastSeq = jsonReader.nextString();
                } else {
                    jsonReader.skipValue();
                }
            }
            jsonReader.endObject();
        } catch(IOException e) {
            throw new DatabaseAccessException(e);
        } finally {
            IOUtils.closeQuietly(jsonReader);
        }
    }

    
    public static class DocumentChange {

        public static final String FIELD_SEQ = "seq";
        public static final String FIELD_ID = "id";
        public static final String FIELD_CHANGES = "changes";
        public static final String FIELD_DELETED = "deleted";
        public static final String FIELD_DOC = "doc";

        private final ObjectParser mParser;
        private JsonObject mJsonObject;
        
        
        public DocumentChange(ObjectParser parser, JsonObject jsonObject) {
            mParser = parser;
            mJsonObject = jsonObject;
        }

        public String getSeq() {
            return mJsonObject.get(FIELD_SEQ).getAsString();
        }

        public long getSeqAsLong() {
            return mJsonObject.get(FIELD_SEQ).getAsLong();
        }

        public String getId() {
            return mJsonObject.get(FIELD_ID).getAsString();
        }

        public List<String> getChanges() throws IOException {
        	List<String> changeRevs = new ArrayList<>();
        	for (ChangeReference ref : getChangesAsReferences()) {
        		changeRevs.add(ref.getRev());
        	}
            return changeRevs;
        }

        public List<ChangeReference> getChangesAsReferences() throws IOException {
            return getChangesAs(ChangeReference.class);
        }

        public <T> List<T> getChangesAs(Class<T> clss) throws IOException {
        	List<T> changeRefs = new ArrayList<>();
            JsonArray array = mJsonObject.get(FIELD_CHANGES).getAsJsonArray();
            for (int i = 0; i < array.size(); i++) {
            	T changeRef = getAsObject(array.get(i), clss);
            	changeRefs.add(changeRef);
            }
            return changeRefs;
        }

        public boolean isDeleted() {
        	JsonElement element = mJsonObject.get(FIELD_DELETED);
            return element != null && element.getAsBoolean();
        }

        public String getDoc() {
            JsonElement element =  mJsonObject.get(FIELD_DOC);
            return element == null ? null : element.toString();
        }

        public JsonElement getDocAsJsonElement() {
            return mJsonObject.get(FIELD_DOC);
        }

        public <T> T getDocAsObject(Class<T> clss) throws IOException {
            return getAsObject(getDocAsJsonElement(), clss);
        }
        
        private <T> T getAsObject(JsonElement element, Class<T> clss) throws IOException {
            if (element == null) {
            	return null;
            }
            if (mParser instanceof GsonParser) {
                return ((GsonParser) mParser).parse(element.toString(), clss);
            } else {
                return mParser.parse(element.toString(), clss);
            }
        }

        @Override
        public String toString() {
            return mJsonObject.toString();
        }
    }
    
    
    public static class ChangeReference {
    	
    	private String rev;
    	
    	public String getRev() {
    		return rev;
    	}
    }
}
