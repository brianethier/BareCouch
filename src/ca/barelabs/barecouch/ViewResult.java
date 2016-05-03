package ca.barelabs.barecouch;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import ca.barelabs.bareconnection.GsonParser;
import ca.barelabs.bareconnection.ObjectParser;
import ca.barelabs.bareconnection.IOUtils;
import ca.barelabs.bareconnection.RestResponse;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;


public class ViewResult implements Iterable<ViewResult.Row> {

    public static final String FIELD_OFFSET = "offset";
    public static final String FIELD_TOTAL_ROWS = "total_rows";
    public static final String FIELD_ROWS = "rows";
    public static final String FIELD_UPDATE_SEQ = "update_seq";

    private final ViewQuery mQuery;
    private final ObjectParser mParser;
    private long mOffset;
    private long mTotalRows;
    private String mUpdateSeq;
    private List<Row> mRows = new ArrayList<Row>();
    

    public ViewResult(ViewQuery query, RestResponse response) throws IOException {
    	mQuery = query;
        mParser = response.getParser();
        parseMetadata(response.parse());
    }
    
    public ViewQuery getQuery() {
    	return mQuery;
    }
    
    public ObjectParser getParser() {
    	return mParser;
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
    
    public <D> List<D> getList(Class<D> clss) throws IOException {
        Iterator<Row> iterator = iterator();
        List<D> list = new ArrayList<D>();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            list.add(mQuery.isIncludeDocs() ? row.getDocAsObject(clss) : row.getValueAsObject(clss));
        }
        return list;
    }
    
    private final void parseMetadata(String result) {
        JsonReader jsonReader = new JsonReader(new StringReader(result));
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
                    mUpdateSeq = jsonReader.nextString();
                } else if (name.equals(FIELD_ROWS)) {
                    jsonReader.beginArray();
                    while(jsonReader.hasNext()) {
                        JsonElement jsonElement = jsonParser.parse(jsonReader);
                        mRows.add(new Row(mParser, jsonElement.getAsJsonObject()));
                    }
                    jsonReader.endArray();
                } else {
                    jsonReader.skipValue();
                }
            }
            jsonReader.endObject();
        }
        catch(IOException e) {
            throw new DatabaseAccessException(e);
        }
        finally {
            IOUtils.closeQuietly(jsonReader);
        }
    }

    
    public static class Row {

        public static final String FIELD_ID = "id";
        public static final String FIELD_KEY = "key";
        public static final String FIELD_VALUE = "value";
        public static final String FIELD_DOC = "doc";
        public static final String FIELD_ERROR = "error";

        private final ObjectParser mParser;
        private final JsonObject mJsonObject;
        
        
        public Row(ObjectParser parser, JsonObject jsonObject) {
            mParser = parser;
            mJsonObject = jsonObject;
        }


        public String getId() {
            return getAsString(FIELD_ID);
        }

        public JsonElement getIdAsJsonElement() {
            return getAsJsonElement(FIELD_ID);
        }

        public <T> T getIdAsObject(Class<T> clss) throws IOException {
            return getAsObject(FIELD_ID, clss);
        }

        public String getKey() {
            return getAsString(FIELD_KEY);
        }

        public JsonElement getKeyAsJsonElement() {
            return getAsJsonElement(FIELD_KEY);
        }

        public <T> T getKeyAsObject(Class<T> clss) throws IOException {
            return getAsObject(FIELD_KEY, clss);
        }

        public String getValue() {
            return getAsString(FIELD_VALUE);
        }

        public JsonElement getValueAsJsonElement() {
            return getAsJsonElement(FIELD_VALUE);
        }

        public <T> T getValueAsObject(Class<T> clss) throws IOException {
            return getAsObject(FIELD_VALUE, clss);
        }

        public String getDoc() {
            return getAsString(FIELD_DOC);
        }

        public JsonElement getDocAsJsonElement() {
            return getAsJsonElement(FIELD_DOC);
        }

        public <T> T getDocAsObject(Class<T> clss) throws IOException {
            return getAsObject(FIELD_DOC, clss);
        }

        public String getError() {
            return getAsString(FIELD_ERROR);
        }

        public JsonElement getErrorAsJsonElement() {
            return mJsonObject.get(FIELD_ERROR);
        }

        public <T> T getErrorAsObject(Class<T> clss) throws IOException {
            return getAsObject(FIELD_ERROR, clss);
        }
        
        private String getAsString(String field) {
            JsonElement element = mJsonObject.get(field);
            return element == null ? null : element.toString();
        }
        
        private JsonElement getAsJsonElement(String field) {
            return mJsonObject.get(field);
        }
        
        private <T> T getAsObject(String field, Class<T> clss) throws IOException {
            JsonElement element = mJsonObject.get(field);
            if (element == null) {
            	return null;
            }
            if (mParser instanceof GsonParser) {
                return ((GsonParser) mParser).parse(element, clss);
            } else {
                return mParser.parse(element.toString(), clss);
            }
        }

        @Override
        public String toString() {
            return mJsonObject.toString();
        }
    }
}

