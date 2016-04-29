package ca.barelabs.barecouch;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import ca.barelabs.bareconnection.ObjectParser;
import ca.barelabs.bareconnection.RestResponse;
import ca.barelabs.barecouch.ChangesResult.DocumentChange;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class StreamingChangesResult implements Closeable {

    private final ObjectParser mParser;
    private final RestResponse mResponse;
    private final JsonParser mJsonParser = new JsonParser();
    private final JsonReader mJsonReader;
    private String mLastSeq;
    private boolean mIteratorCreated;
    private boolean mClosed;
    private boolean mAllChangesRead;
    

    public StreamingChangesResult(ObjectParser parser, RestResponse response) throws UnsupportedEncodingException, IOException {
        mParser = parser;
        mResponse = response;
        mJsonReader = new JsonReader(new InputStreamReader(mResponse.getContent(), mResponse.getIncomingCharset()));
        parseMetadata(mJsonReader);
    }

    public String getLastSeq() {
    	if (!mIteratorCreated || !mAllChangesRead) {
            throw new IllegalStateException("You must read through all values returned from iterator first.");
    	}
    	if (mLastSeq == null) {
            parseLastSeq(mJsonReader);
    	}
        return mLastSeq;
    }

    public Iterator<ChangesResult.DocumentChange> iterator() {
        if (mClosed) {
            throw new IllegalStateException("Access to iterator is not possible after view result was closed or disconnected.");
        }
        if (mIteratorCreated) {
            throw new IllegalStateException("Iterator can only be called once!");
        }
        mIteratorCreated = true;
        return new StreamingChangesResultIterator();
    }
    
    @Override
    public void close() {
        mClosed = true;
        mResponse.disconnect();
    }
    
    private void parseMetadata(JsonReader jsonReader) {
        try {
            jsonReader.beginObject();
            while (jsonReader.hasNext()) {
                String name = jsonReader.nextName();
                if (name.equals(ChangesResult.FIELD_RESULTS)) {
                    jsonReader.beginArray();
                    // We are now ready to start reading rows
                    return;
                } else {
                    jsonReader.skipValue();
                }
            }
        } catch (IOException e) {
            throw new DatabaseAccessException(e);
        }
    }
    
    private void parseLastSeq(JsonReader jsonReader) {
        try {
            if (mClosed) {
                throw new IllegalStateException("Access to last sequence can not longer be read after result was closed or disconnected.");
            }
            jsonReader.endArray();
            while (jsonReader.hasNext()) {
                String name = jsonReader.nextName();
                if (name.equals(ChangesResult.FIELD_LAST_SEQ)) {
                    mLastSeq = jsonReader.nextString();
                } else {
                    jsonReader.skipValue();
                }
            }
        } catch (IOException e) {
            throw new DatabaseAccessException(e);
        }
    }


    private class StreamingChangesResultIterator implements Iterator<DocumentChange> {
        
        public boolean hasNext() {
            try {
                if (mJsonReader.hasNext()) {
                	return true;
                }
                mAllChangesRead = true;
                return false;
            } catch (IOException e) {
                throw new DatabaseAccessException(e);
            }
        }

        public DocumentChange next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Attempt to iterate beyond the result set.");
            }
            JsonElement jsonElement = mJsonParser.parse(mJsonReader);
            return new DocumentChange(mParser, jsonElement.getAsJsonObject());
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
