package ca.barelabs.barecouch;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import ca.barelabs.bareconnection.ObjectParser;
import ca.barelabs.bareconnection.RestResponse;
import ca.barelabs.barecouch.ViewResult.Row;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class StreamingViewResult implements Closeable {

    private final ObjectParser mParser;
    private final RestResponse mResponse;
    private final JsonParser mJsonParser = new JsonParser();
    private final JsonReader mJsonReader;
    private long mOffset;
    private long mTotalRows;
    private String mUpdateSeq;
    private boolean mIteratorCreated;
    private boolean mClosed;
    

    public StreamingViewResult(ObjectParser parser, RestResponse response) throws UnsupportedEncodingException, IOException {
        mParser = parser;
        mResponse = response;
        mJsonReader = new JsonReader(new InputStreamReader(mResponse.getContent(), mResponse.getIncomingCharset()));
        parseMetadata(mJsonReader);
    }
    

    public long getOffset() {
        return mOffset;
    }

    public long getTotalRows() {
        return mTotalRows;
    }

    public String getUpdateSeq() {
        return mUpdateSeq;
    }

    public Iterator<ViewResult.Row> iterator() {
        if (mClosed) {
            throw new IllegalStateException("Access to iterator is not possible after view result was closed or disconnected.");
        }
        if (mIteratorCreated) {
            throw new IllegalStateException("Iterator can only be called once!");
        }
        mIteratorCreated = true;
        return new StreamingViewResultIterator();
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
                if (name.equals(ViewResult.FIELD_OFFSET)) {
                    mOffset = jsonReader.nextLong();
                } else if (name.equals(ViewResult.FIELD_TOTAL_ROWS)) {
                    mTotalRows = jsonReader.nextLong();
                } else if (name.equals(ViewResult.FIELD_UPDATE_SEQ)) {
                    mUpdateSeq = jsonReader.nextString();
                } else if (name.equals(ViewResult.FIELD_ROWS)) {
                    jsonReader.beginArray();
                    // We are now ready to start reading rows
                    return;
                } else {
                    jsonReader.skipValue();
                }
            }
        }
        catch (IOException e) {
            throw new DatabaseAccessException(e);
        }
    }


    private class StreamingViewResultIterator implements Iterator<Row> {
        
        public boolean hasNext() {
            try {
                return mJsonReader.hasNext();
            } catch (IOException e) {
                throw new DatabaseAccessException(e);
            }
        }

        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Attempt to iterate beyond the result set.");
            }
            JsonElement jsonElement = mJsonParser.parse(mJsonReader);
            return new Row(mParser, jsonElement.getAsJsonObject());
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
