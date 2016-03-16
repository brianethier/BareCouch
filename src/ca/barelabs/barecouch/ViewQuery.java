package ca.barelabs.barecouch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import ca.barelabs.bareconnection.RestConnection;
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
    
    private final static int NOT_SET = -1;

    private final Gson mGson;
    private final Map<String, String> mQueryParams = new TreeMap<String, String>();
    private String mDesignDocId;
    private String mViewName;
    private String mListName;
    private Keys mKeys = new Keys();
    private Object mKey;
    private Object mStartKey;
    private String mStartDocId;
    private Object mEndKey;
    private String mEndDocId;
    private boolean mAllDocs;
    private boolean mStaleOk;
    private boolean mStaleOkUpdateAfter;
    private boolean mDescending;
    private boolean mGroup;
    private boolean mReduce = true;
    private boolean mIncludeDocs = false;
    private boolean mInclusiveEnd = true;
    private boolean mUpdateSeq = false;
    private int mLimit = NOT_SET;
    private int mSkip = NOT_SET;
    private int mGroupLevel = NOT_SET;

    
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
        return mDesignDocId;
    }

    public ViewQuery designDocId(String designDocId) {
        this.mDesignDocId = designDocId;
        return this;
    }

    public String getViewName() {
        return mViewName;
    }

    public ViewQuery viewName(String viewName) {
        mViewName = viewName;
        return this;
    }

    public String getListName() {
        return mListName;
    }

    public ViewQuery listName(String listName) {
        mListName = listName;
        return this;
    }

    public Keys getKeys() {
        return mKeys;
    }

    public boolean hasMultipleKeys() {
        return !mKeys.getValues().isEmpty();
    }

    public Collection<?> getKeysValues() {
        return mKeys.getValues();
    }

    public String getKeysAsJson() {
        return mGson.toJson(mKeys);
    }

    public ViewQuery keys(Collection<?> keyList) {
        mKeys.setValues(keyList);
        return this;
    }

    public Object getKey() {
        return mKey;
    }

    public ViewQuery key(Object object) {
        mKey = object;
        return this;
    }

    public ViewQuery key(String value) {
        mKey = value;
        return this;
    }

    public ViewQuery key(int value) {
        mKey = value;
        return this;
    }

    public ViewQuery key(long value) {
        mKey = value;
        return this;
    }
    
    public ViewQuery key(float value) {
        mKey = value;
        return this;
    }
    
    public ViewQuery key(double value) {
        mKey = value;
        return this;
    }

    public ViewQuery key(boolean value) {
        mKey = value;
        return this;
    }
    
    public ViewQuery keyFromJson(String json, Class<?> clss) {
        mKey = mGson.fromJson(json, clss);
        return this;
    }

    public Object getStartKey() {
        return mStartKey;
    }

    public ViewQuery startKey(Object object) {
        mStartKey = object;
        return this;
    }

    public ViewQuery startKey(String value) {
        mStartKey = value;
        return this;
    }

    public ViewQuery startKey(int value) {
        mStartKey = value;
        return this;
    }

    public ViewQuery startKey(long value) {
        mStartKey = value;
        return this;
    }

    public ViewQuery startKey(float value) {
        mStartKey = value;
        return this;
    }

    public ViewQuery startKey(double value) {
        mStartKey = value;
        return this;
    }

    public ViewQuery startKey(boolean value) {
        mStartKey = value;
        return this;
    }

    public ViewQuery startKeyFromJson(String json, Class<?> clss) {
        mStartKey = mGson.fromJson(json, clss);
        return this;
    }

    public String getStartDocId() {
        return mStartDocId;
    }

    public ViewQuery startDocId(String startDocId) {
        mStartDocId = startDocId;
        return this;
    }

    public Object getEndKey() {
        return mEndKey;
    }

    public ViewQuery endKey(Object object) {
        mEndKey = object;
        return this;
    }
    
    public ViewQuery endKey(String value) {
        mEndKey = value;
        return this;
    }

    public ViewQuery endKey(int value) {
        mEndKey = value;
        return this;
    }

    public ViewQuery endKey(long value) {
        mEndKey = value;
        return this;
    }

    public ViewQuery endKey(float value) {
        mEndKey = value;
        return this;
    }

    public ViewQuery endKey(double value) {
        mEndKey = value;
        return this;
    }

    public ViewQuery endKey(boolean value) {
        mEndKey = value;
        return this;
    }

    public ViewQuery endKeyFromJson(String json, Class<?> clss) {
        mEndKey = mGson.fromJson(json, clss);
        return this;
    }

    public String getEndDocId() {
        return mEndDocId;
    }

    public ViewQuery endDocId(String endDocId) {
        mEndDocId = endDocId;
        return this;
    }
    
    public boolean isAllDocs() {
        return mAllDocs;
    }
    
    public ViewQuery allDocs(boolean allDocs) {
        mAllDocs = allDocs;
        return this;
    }

    public boolean isStaleOk() {
        return mStaleOk;
    }

    public ViewQuery staleOk(boolean staleOk) {
        mStaleOk = staleOk;
        return this;
    }

    public boolean isStaleOkUpdateAfter() {
        return mStaleOkUpdateAfter;
    }

    public ViewQuery staleOkUpdateAfter(boolean staleOkUpdateAfter) {
        mStaleOkUpdateAfter = staleOkUpdateAfter;
        return this;
    }

    public boolean isDescending() {
        return mDescending;
    }

    public ViewQuery descending(boolean descending) {
        mDescending = descending;
        return this;
    }

    public boolean isGroup() {
        return mGroup;
    }

    public ViewQuery group(boolean group) {
        mGroup = group;
        return this;
    }

    public boolean isReduce() {
        return mReduce;
    }

    public ViewQuery reduce(boolean reduce) {
        mReduce = reduce;
        return this;
    }

    public boolean isIncludeDocs() {
        return mIncludeDocs;
    }

    public ViewQuery includeDocs(boolean includeDocs) {
        mIncludeDocs = includeDocs;
        return this;
    }

    public boolean isInclusiveEnd() {
        return mInclusiveEnd;
    }

    public ViewQuery inclusiveEnd(boolean inclusiveEnd) {
        mInclusiveEnd = inclusiveEnd;
        return this;
    }

    public boolean isUpdateSeq() {
        return mUpdateSeq;
    }

    public ViewQuery updateSeq(boolean updateSeq) {
        mUpdateSeq = updateSeq;
        return this;
    }

    public int getLimit() {
        return mLimit;
    }

    public ViewQuery limit(int limit) {
        mLimit = limit;
        return this;
    } 

    public int getSkip() {
        return mSkip;
    }

    public ViewQuery skip(int skip) {
        mSkip = skip;
        return this;
    }

    public int getGroupLevel() {
        return mGroupLevel;
    }

    public ViewQuery groupLevel(int groupLevel) {
        mGroupLevel = groupLevel;
        return this;
    }

    public String buildQuery() throws IOException {
        Map<String, String> params = createParams();
        return createViewPath() + "?" + RestUtils.toQuery(params, RestConnection.DEFAULT_CHARSET);
    }

    private Map<String, String> createParams() {
        HashMap<String, String> params = new HashMap<String, String>();
        if (mKey != null) {
            params.put(PARAM_KEY, mGson.toJson(mKey));
        }
        if (mStartKey != null) {
            params.put(PARAM_STARTKEY, mGson.toJson(mStartKey));
        }
        if (mStartDocId != null) {
            params.put(PARAM_STARTKEY_DOCID, mStartDocId);
        }
        if (mEndKey != null) {
            params.put(PARAM_ENDKEY, mGson.toJson(mEndKey));
        }
        if (mEndDocId != null) {
            params.put(PARAM_ENDKEY_DOCID, mEndDocId);
        }
        if (mStaleOk) {
            params.put(PARAM_STALE, VALUE_STALE_OK);
        }
        if (mStaleOkUpdateAfter) {
            params.put(PARAM_STALE, VALUE_STALE_UPDATE_AFTER);
        }
        if (mDescending) {
            params.put(PARAM_DESCENDING, String.valueOf(mDescending));
        }
        if (!mInclusiveEnd) {
            params.put(PARAM_INCLUSIVE_END, String.valueOf(mInclusiveEnd));
        }
        if (!mReduce) {
            params.put(PARAM_REDUCE, String.valueOf(mReduce));
        }
        if (mIncludeDocs) {
            params.put(PARAM_INCLUDE_DOCS, String.valueOf(mIncludeDocs));
        }
        if (mGroup) {
            params.put(PARAM_GROUP, String.valueOf(mGroup));
        }
        if (mUpdateSeq) {
            params.put(PARAM_UPDATE_SEQ, String.valueOf(mUpdateSeq));
        }
        if (mLimit != NOT_SET) {
            params.put(PARAM_LIMIT, String.valueOf(mLimit));
        }
        if (mSkip != NOT_SET) {
            params.put(PARAM_SKIP, String.valueOf(mSkip));
        }
        if (mGroupLevel != NOT_SET) {
            params.put(PARAM_GROUP_LEVEL, String.valueOf(mGroupLevel));
        }
        for (Map.Entry<String, String> param : mQueryParams.entrySet()) {
            params.put(param.getKey(), param.getValue());
        }
        return params;
    }

    private String createViewPath() {
        ensureValidViewPath();
        if (mAllDocs) {
            return ALL_DOCS_PATH;
        }
        StringBuilder sb = new StringBuilder();
        if (mListName == null) {
            sb.append(RestConnection.PATH_SEPARATOR).append(mDesignDocId)
                .append(RestConnection.PATH_SEPARATOR).append("_view")
                .append(RestConnection.PATH_SEPARATOR).append(mViewName);
        } else {
            sb.append(RestConnection.PATH_SEPARATOR).append(mDesignDocId)
                .append(RestConnection.PATH_SEPARATOR).append("_list")
                .append(RestConnection.PATH_SEPARATOR).append(mListName)
                .append(RestConnection.PATH_SEPARATOR).append(mViewName);
        }
        return sb.toString();
    }
    
    private void ensureValidViewPath() {
        if (!mAllDocs && (mDesignDocId == null || mDesignDocId.length() == 0 || mViewName == null || mViewName.length() == 0)) {
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