package com.barenode.barecouch;

import java.io.Closeable;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.NoSuchElementException;

import ca.barelabs.bareconnection.RestConnection;

import com.barenode.barecouch.ViewResult.Row;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class StreamingViewResult implements Closeable {

    private final RestConnection mConnection;
    private final JsonParser mJsonParser = new JsonParser();
    private final JsonReader mJsonReader;
    private long mOffset;
    private long mTotalRows;
    private String mUpdateSeq;
    private boolean mIteratorCreated;
    private boolean mClosed;
    

    public StreamingViewResult(RestConnection connection, Reader reader) {
        mConnection = connection;
        mJsonReader = new JsonReader(reader);
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
        if(mClosed) {
            throw new IllegalStateException("Access to iterator is not possible after view result was closed or disconnected.");
        }
        if(mIteratorCreated) {
            throw new IllegalStateException("Iterator can only be called once!");
        }
        mIteratorCreated = true;
        return new StreamingViewResultIterator();
    }
    
    @Override
    public void close() {
        try {
            mClosed = true;
            mJsonReader.close();
        } catch (IOException e) { /* Nothing else we could do here */ }
        mConnection.disconnect();
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
                    mUpdateSeq = Long.toString(jsonReader.nextLong());
                } else if (name.equals(ViewResult.FIELD_ROWS)) {
                    jsonReader.beginArray();
                    // We are now ready to start reading rows
                    return;
                } else {
                    jsonReader.skipValue();
                }
            }
        }
        catch(IOException e) {
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
            if(!hasNext()) {
                throw new NoSuchElementException("Attempt to iterate beyond the result set.");
            }
            JsonElement jsonElement = mJsonParser.parse(mJsonReader);
            return new Row(jsonElement.getAsJsonObject());
        }
        
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
