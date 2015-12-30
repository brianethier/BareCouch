package ca.barelabs.barecouch;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import ca.barelabs.bareconnection.RestConnection;
import ca.barelabs.bareconnection.RestException;
import ca.barelabs.bareconnection.RestUtils;

import com.google.gson.Gson;

public class ViewQuery {

    public final static String ALL_DOCS_PATH = "/_all_docs";
    public final static String PARAM_KEY = "key";
    public final static String PARAM_STARTKEY = "startkey";
    public final static String PARAM_STARTKEY_DOCID = "startkey_docid";
    public final static String PARAM_ENDKEY = "endkey";
    public final static String PARAM_ENDKEY_DOCID = "endkey_docid";
    public final static String PARAM_STALE = "stale";
    public final static String PARAM_DESCENDING = "descending";
    public final static String PARAM_INCLUSIVE_END = "inclusive_end";
    public final static String PARAM_REDUCE = "reduce";
    public final static String PARAM_INCLUDE_DOCS = "include_docs";
    public final static String PARAM_GROUP = "group";
    public final static String PARAM_UPDATE_SEQ = "update_seq";
    public final static String PARAM_LIMIT = "limit";
    public final static String PARAM_SKIP = "skip";
    public final static String PARAM_GROUP_LEVEL = "group_level";
    public final static String VALUE_STALE_OK = "ok";
    public final static String VALUE_STALE_UPDATE_AFTER = "update_after";
    public final static String VALUE_TRUE = "true";
    public final static String VALUE_FALSE = "false";
    public final static String SEPARATOR = "/";
    private final static int NOT_SET = -1;

    private final Gson mGson;
    private final Map<String, String> mQueryParams = new TreeMap<String, String>();
    private String designDocId;
    private String viewName;
    private String listName;
    private Keys keys = new Keys();
    private Object key;
    private Object startKey;
    private String startDocId;
    private Object endKey;
    private String endDocId;
    private boolean allDocs;
    private boolean staleOk;
    private boolean staleOkUpdateAfter;
    private boolean descending;
    private boolean group;
    private boolean reduce = true;
    private boolean includeDocs = false;
    private boolean inclusiveEnd = true;
    private boolean updateSeq = false;
    private int limit = NOT_SET;
    private int skip = NOT_SET;
    private int groupLevel = NOT_SET;

    
    /**
     * Provide your own Gson parser to use when serializing keys when building query.
     * @param gson
     */
    public ViewQuery(Gson gson) {
        mGson = gson;
    }
    
    public ViewQuery() {
        this(new Gson());
    }


    public ViewQuery param(String name, String value) {
        mQueryParams.put(name, value);
        return this;
    }

    public String getDesignDocId() {
        return designDocId;
    }

    public ViewQuery designDocId(String designDocId) {
        this.designDocId = designDocId;
        return this;
    }

    public String getViewName() {
        return viewName;
    }

    public ViewQuery viewName(String viewName) {
        this.viewName = viewName;
        return this;
    }

    public String getListName() {
        return listName;
    }

    public ViewQuery listName(String listName) {
        this.listName = listName;
        return this;
    }

    public Keys getKeys() {
        return keys;
    }

    public boolean hasMultipleKeys() {
        return !keys.getValues().isEmpty();
    }

    public Collection<?> getKeysValues() {
        return keys.getValues();
    }

    public String getKeysAsJson() {
        return mGson.toJson(keys);
    }

    public ViewQuery keys(Collection<?> keyList) {
        keys.setValues(keyList);
        return this;
    }

    public Object getKey() {
        return key;
    }

    public ViewQuery key(Object object) {
        this.key = object;
        return this;
    }

    public ViewQuery key(String value) {
        this.key = value;
        return this;
    }

    public ViewQuery key(int value) {
        this.key = value;
        return this;
    }

    public ViewQuery key(long value) {
        this.key = value;
        return this;
    }
    
    public ViewQuery key(float value) {
        this.key = value;
        return this;
    }
    
    public ViewQuery key(double value) {
        this.key = value;
        return this;
    }

    public ViewQuery key(boolean value) {
        this.key = value;
        return this;
    }
    
    public ViewQuery keyFromJson(String json, Class<?> clss) {
        this.key = mGson.fromJson(json, clss);
        return this;
    }

    public Object getStartKey() {
        return startKey;
    }

    public ViewQuery startKey(Object object) {
        this.startKey = object;
        return this;
    }

    public ViewQuery startKey(String value) {
        this.startKey = value;
        return this;
    }

    public ViewQuery startKey(int value) {
        this.startKey = value;
        return this;
    }

    public ViewQuery startKey(long value) {
        this.startKey = value;
        return this;
    }

    public ViewQuery startKey(float value) {
        this.startKey = value;
        return this;
    }

    public ViewQuery startKey(double value) {
        this.startKey = value;
        return this;
    }

    public ViewQuery startKey(boolean value) {
        this.startKey = value;
        return this;
    }

    public ViewQuery startKeyFromJson(String json, Class<?> clss) {
        this.startKey = mGson.fromJson(json, clss);
        return this;
    }

    public String getStartDocId() {
        return startDocId;
    }

    public ViewQuery startDocId(String startDocId) {
        this.startDocId = startDocId;
        return this;
    }

    public Object getEndKey() {
        return endKey;
    }

    public ViewQuery endKey(Object object) {
        this.endKey = object;
        return this;
    }
    
    public ViewQuery endKey(String value) {
        this.endKey = value;
        return this;
    }

    public ViewQuery endKey(int value) {
        this.endKey = value;
        return this;
    }

    public ViewQuery endKey(long value) {
        this.endKey = value;
        return this;
    }

    public ViewQuery endKey(float value) {
        this.endKey = value;
        return this;
    }

    public ViewQuery endKey(double value) {
        this.endKey = value;
        return this;
    }

    public ViewQuery endKey(boolean value) {
        this.endKey = value;
        return this;
    }

    public ViewQuery endKeyFromJson(String json, Class<?> clss) {
        this.endKey = mGson.fromJson(json, clss);
        return this;
    }

    public String getEndDocId() {
        return endDocId;
    }

    public ViewQuery endDocId(String endDocId) {
        this.endDocId = endDocId;
        return this;
    }
    
    public boolean isAllDocs() {
        return allDocs;
    }
    
    public ViewQuery allDocs(boolean allDocs) {
        this.allDocs = allDocs;
        return this;
    }

    public boolean isStaleOk() {
        return staleOk;
    }

    public ViewQuery staleOk(boolean staleOk) {
        this.staleOk = staleOk;
        return this;
    }

    public boolean isStaleOkUpdateAfter() {
        return staleOkUpdateAfter;
    }

    public ViewQuery staleOkUpdateAfter(boolean staleOkUpdateAfter) {
        this.staleOkUpdateAfter = staleOkUpdateAfter;
        return this;
    }

    public boolean isDescending() {
        return descending;
    }

    public ViewQuery descending(boolean descending) {
        this.descending = descending;
        return this;
    }

    public boolean isGroup() {
        return group;
    }

    public ViewQuery group(boolean group) {
        this.group = group;
        return this;
    }

    public boolean isReduce() {
        return reduce;
    }

    public ViewQuery reduce(boolean reduce) {
        this.reduce = reduce;
        return this;
    }

    public boolean isIncludeDocs() {
        return includeDocs;
    }

    public ViewQuery includeDocs(boolean includeDocs) {
        this.includeDocs = includeDocs;
        return this;
    }

    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

    public ViewQuery inclusiveEnd(boolean inclusiveEnd) {
        this.inclusiveEnd = inclusiveEnd;
        return this;
    }

    public boolean isUpdateSeq() {
        return updateSeq;
    }

    public ViewQuery updateSeq(boolean updateSeq) {
        this.updateSeq = updateSeq;
        return this;
    }

    public int getLimit() {
        return limit;
    }

    public ViewQuery limit(int limit) {
        this.limit = limit;
        return this;
    } 

    public int getSkip() {
        return skip;
    }

    public ViewQuery skip(int skip) {
        this.skip = skip;
        return this;
    }

    public int getGroupLevel() {
        return groupLevel;
    }

    public ViewQuery groupLevel(int groupLevel) {
        this.groupLevel = groupLevel;
        return this;
    }

    public String buildQuery() throws RestException {
        try {
            Map<String, String> params = createParams();
            return createViewPath() + "?" + RestUtils.buildQuery(params, RestConnection.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RestException(RestConnection.SC_UNKNOWN, e);
        }
    }

    private Map<String, String> createParams() {
        HashMap<String, String> params = new HashMap<String, String>();
        if (key != null) {
            params.put(PARAM_KEY, mGson.toJson(key));
        }
        if (startKey != null) {
            params.put(PARAM_STARTKEY, mGson.toJson(startKey));
        }
        if (startDocId != null) {
            params.put(PARAM_STARTKEY_DOCID, startDocId);
        }
        if (endKey != null) {
            params.put(PARAM_ENDKEY, mGson.toJson(endKey));
        }
        if (endDocId != null) {
            params.put(PARAM_ENDKEY_DOCID, endDocId);
        }
        if (staleOk) {
            params.put(PARAM_STALE, VALUE_STALE_OK);
        }
        if (staleOkUpdateAfter) {
            params.put(PARAM_STALE, VALUE_STALE_UPDATE_AFTER);
        }
        if (descending) {
            params.put(PARAM_DESCENDING, VALUE_TRUE);
        }
        if (!inclusiveEnd) {
            params.put(PARAM_INCLUSIVE_END, VALUE_FALSE);
        }
        if (!reduce) {
            params.put(PARAM_REDUCE, VALUE_FALSE);
        }
        if (includeDocs) {
            params.put(PARAM_INCLUDE_DOCS, VALUE_TRUE);
        }
        if (group) {
            params.put(PARAM_GROUP, VALUE_TRUE);
        }
        if (updateSeq) {
            params.put(PARAM_UPDATE_SEQ, VALUE_TRUE);
        }
        if (limit != NOT_SET) {
            params.put(PARAM_LIMIT, String.valueOf(limit));
        }
        if (skip != NOT_SET) {
            params.put(PARAM_SKIP, String.valueOf(skip));
        }
        if (groupLevel != NOT_SET) {
            params.put(PARAM_GROUP_LEVEL, String.valueOf(groupLevel));
        }
        for (Map.Entry<String, String> param : mQueryParams.entrySet()) {
            params.put(param.getKey(), param.getValue());
        }
        return params;
    }

    private String createViewPath() {
        ensureValidViewPath();
        if (allDocs) {
            return ALL_DOCS_PATH;
        }
        StringBuilder sb = new StringBuilder();
        if (listName == null) {
            sb.append(SEPARATOR).append(designDocId)
                .append(SEPARATOR).append("_view")
                .append(SEPARATOR).append(viewName);
        } else {
            sb.append(SEPARATOR).append(designDocId)
                .append(SEPARATOR).append("_list")
                .append(SEPARATOR).append(listName)
                .append(SEPARATOR).append(viewName);
        }
        return sb.toString();
    }
    
    private void ensureValidViewPath() {
        if (!allDocs && (designDocId == null || designDocId.length() == 0 || viewName == null || viewName.length() == 0)) {
            throw new IllegalStateException("You must call designDocId(...) and viewName(...) with non-empty values.");
        }
    }
    

    public static class Keys {

        private final List<Object> keys = new ArrayList<Object>();
        
        public void setValues(Collection<?> keys) {
            this.keys.clear();
            this.keys.addAll(keys);
        }

        public List<?> getValues() {
            return Collections.unmodifiableList(keys);
        }
    }



}