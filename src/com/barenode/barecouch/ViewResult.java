package com.barenode.barecouch;


import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;


public class ViewResult implements Iterable<ViewResult.Row> {

    public static final String FIELD_OFFSET = "offset";
    public static final String FIELD_TOTAL_ROWS = "total_rows";
    public static final String FIELD_ROWS = "rows";
    public static final String FIELD_UPDATE_SEQ = "update_seq";

    private long mOffset;
    private long mTotalRows;
    private String mUpdateSeq;
    private List<Row> mRows = new ArrayList<Row>();
    

    public ViewResult(Reader reader) {
        parseMetadata(new JsonReader(reader));
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

    public List<Row> getRows() {
        return mRows;
    }

    public int getSize() {
        return mRows.size();
    }

    public Iterator<ViewResult.Row> iterator() {
        return mRows.iterator();
    }

    public boolean isEmpty() {
        return mRows.isEmpty();
    }
    
    private final void parseMetadata(JsonReader jsonReader) {
        JsonParser jsonParser = new JsonParser();
        try {
            jsonReader.beginObject();
            while (jsonReader.hasNext()) {
                String name = jsonReader.nextName();
                if (name.equals(FIELD_OFFSET)) {
                    mOffset = jsonReader.nextLong();
                } else if (name.equals(FIELD_TOTAL_ROWS)) {
                    mTotalRows = jsonReader.nextLong();
                } else if (name.equals(FIELD_UPDATE_SEQ)) {
                    mUpdateSeq = Long.toString(jsonReader.nextLong());
                } else if (name.equals(FIELD_ROWS)) {
                    jsonReader.beginArray();
                    while(jsonReader.hasNext()) {
                        JsonElement jsonElement = jsonParser.parse(jsonReader);
                        mRows.add(new Row(jsonElement.getAsJsonObject()));
                    }
                    jsonReader.endArray();
                } else {
                    jsonReader.skipValue();
                }
            }
            jsonReader.endObject();
        }
        catch(IOException e) {
            throw new DbAccessException(e);
        }
        finally {
            try {
                jsonReader.close();
            } catch (IOException e) { /* Nothing we can do */ }
        }
    }

    
    public static class Row {

        public static final String FIELD_ID = "id";
        public static final String FIELD_KEY = "key";
        public static final String FIELD_VALUE = "value";
        public static final String FIELD_DOC = "doc";
        public static final String FIELD_ERROR = "error";

        private Gson mGson = new Gson();;
        private JsonObject mJsonObject;
        
        
        public Row(JsonObject jsonObject) {
            mJsonObject = jsonObject;
        }


        public String getId() {
            return getAsString(FIELD_ID);
        }

        public <T> T getIdAsObject(Class<T> clss) {
            return getAsObject(FIELD_ID, clss);
        }

        public String getKey() {
            return getAsString(FIELD_KEY);
        }

        public <T> T getKeyAsObject(Class<T> clss) {
            return getAsObject(FIELD_KEY, clss);
        }

        public String getValue() {
            return getAsString(FIELD_VALUE);
        }

        public <T> T getValueAsObject(Class<T> clss) {
            return getAsObject(FIELD_VALUE, clss);
        }

        public String getDoc() {
            return getAsString(FIELD_DOC);
        }

        public <T> T getDocAsObject(Class<T> clss) {
            return getAsObject(FIELD_DOC, clss);
        }

        public String getError() {
            return getAsString(FIELD_ERROR);
        }

        public <T> T getErrorAsObject(Class<T> clss) {
            return getAsObject(FIELD_ERROR, clss);
        }
        
        private String getAsString(String field) {
            JsonElement element = mJsonObject.get(field);
            return element == null ? null : element.toString();
        }
        
        private <T> T getAsObject(String field, Class<T> clss) {
            JsonElement element = mJsonObject.get(field);
            return element == null ? null : mGson.fromJson(element, clss);
        }

        @Override
        public String toString() {
            return mJsonObject.toString();
        }
    }
}

