package ca.barelabs.barecouch;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
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

    private final static String ALL_DOCS_VIEW_NAME = "_all_docs";
    private final static String SEPARATOR = "/";
    private final static int NOT_SET = -1;

    private final Gson mGson;
    private final Map<String, String> mQueryParams = new TreeMap<String, String>();

    private String designDocId;
    private String viewName;
    private Object key;
    private Keys keys;
    private Object startKey;
    private String startDocId;
    private Object endKey;
    private String endDocId;
    private int limit = NOT_SET;
    private String staleOk;
    private boolean descending;
    private int skip = NOT_SET;
    private boolean group;
    private int groupLevel = NOT_SET;
    private boolean reduce = true;
    private boolean includeDocs = false;
    private boolean inclusiveEnd = true;
    private boolean ignoreNotFound = false;
    private boolean updateSeq = false;

    private boolean cacheOk = false;

    private String cachedQuery;
    private String listName;

    
    public ViewQuery() {
        mGson = new Gson();
    }
    
    /**
     * Provide your own Gson parser. Used when serializing keys when building the query.
     * @param gson
     */
    public ViewQuery(Gson gson) {
        mGson = gson;
    }
    
    


    public String getDesignDocId() {
        return designDocId;
    }

    public String getViewName() {
        return viewName;
    }

    public String getStartDocId() {
        return startDocId;
    }

    public String getEndDocId() {
        return endDocId;
    }

    public int getLimit() {
        return limit;
    }

    public boolean isStaleOk() {
        return staleOk != null && ("ok".equals(staleOk) || "update_after".equals(staleOk));
    }

    public boolean isDescending() {
        return descending;
    }

    public int getSkip() {
        return skip;
    }

    public boolean isGroup() {
        return group;
    }

    public int getGroupLevel() {
        return groupLevel;
    }

    public boolean isReduce() {
        return reduce;
    }

    public boolean isIncludeDocs() {
        return includeDocs;
    }

    public boolean isInclusiveEnd() {
        return inclusiveEnd;
    }

    public boolean isUpdateSeq() {
        return updateSeq;
    }


    public ViewQuery designDocId(String s) {
        reset();
        designDocId = s;
        return this;
    }
    /**
     * Will automatically set the query special _all_docs URI.
     * In this case, setting designDocId will have no effect.
     * @return
     */
    public ViewQuery allDocs() {
        reset();
        viewName = ALL_DOCS_VIEW_NAME;
        return this;
    }
    
    
    public boolean isAllDocs() {
        return viewName != null && viewName.equals(ALL_DOCS_VIEW_NAME);
    }

    public ViewQuery viewName(String s) {
        reset();
        viewName = s;
        return this;
    }

    public ViewQuery listName(String s) {
        reset();
        listName = s;
        return this;
    }
    /**
     * If set to true, the view query result will be cached and subsequent queries
     * (with cacheOk set) may be served from the cache instead of the db.
     *
     * Note that if the view changes, the cache will be invalidated.
     *
     * @param b
     * @return
     */
    public ViewQuery cacheOk(boolean b) {
        reset();
        cacheOk = b;
        return this;
    }

    public boolean isCacheOk() {
        return cacheOk;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(String s) {
        reset();
        key = s;
        return this;
    }
    /**
     * @param Will be parsed as JSON.
     * @return the view query for chained calls
     */
    public ViewQuery rawKey(String s, Class<?> clss) {
        reset();
        key = mGson.fromJson(s, clss);
        return this;
    }

    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(int i) {
        reset();
        key = i;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(long l) {
        reset();
        key = l;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(float f) {
        reset();
        key = f;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(double d) {
        reset();
        key = d;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(boolean b) {
        reset();
        key = b;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery key(Object o) {
        reset();
        key = o;
        return this;
    }
    /**
     * For multiple-key queries (as of CouchDB 0.9). Keys will be JSON-encoded.
     * @param keyList a list of Object, will be JSON encoded according to each element's type.
     * @return the view query for chained calls
     */
    public ViewQuery keys(Collection<?> keyList) {
        reset();
        keys = Keys.of(keyList);
        return this;
    }

    public Keys getKeys() {
        return keys;
    }

    public Collection<?> getKeysValues() {
        return keys.getValues();
    }

    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(String s) {
        reset();
        startKey = s;
        return this;
    }

    /**
     * @param Will be parsed as json
     * @return the view query for chained calls
     */
    public ViewQuery rawStartKey(String s, Class<?> clss) {
        reset();
        startKey = mGson.fromJson(s, clss);
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(int i) {
        reset();
        startKey = i;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(long l) {
        reset();
        startKey = l;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(float f) {
        reset();
        startKey = f;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(double d) {
        reset();
        startKey = d;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(boolean b) {
        reset();
        startKey = b;
        return this;
    }

    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery startKey(Object o) {
        reset();
        startKey = o;
        return this;
    }

    public ViewQuery startDocId(String s) {
        reset();
        startDocId = s;
        return this;
    }
    /**
     * @param will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(String s) {
        reset();
        endKey = s;
        return this;
    }
    /**
     * @param will be parsed as JSON.
     * @return the view query for chained calls
     */
    public ViewQuery rawEndKey(String s, Class<?> clss) {
        reset();
        endKey = mGson.fromJson(s, clss);
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(int i) {
        reset();
        endKey = i;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(long l) {
        reset();
        endKey = l;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(float f) {
        reset();
        endKey = f;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(double d) {
        reset();
        endKey = d;
        return this;
    }
    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(boolean b) {
        reset();
        endKey = b;
        return this;
    }

    /**
     * @param Will be JSON-encoded.
     * @return the view query for chained calls
     */
    public ViewQuery endKey(Object o) {
        reset();
        endKey = o;
        return this;
    }

    public ViewQuery endDocId(String s) {
        reset();
        endDocId = s;
        return this;
    }
    /**
     * limit=0 you don't get any data, but all meta-data for this View. The number of documents in this View for example.
     * @param i the limit
     * @return the view query for chained calls
     */
    public ViewQuery limit(int i) {
        reset();
        limit = i;
        return this;
    }
    /**
     * The stale option can be used for higher performance at the cost of possibly not seeing the all latest data. If you set the stale option to ok, CouchDB may not perform any refreshing on the view that may be necessary.
     * @param b the staleOk flag
     * @return the view query for chained calls
     */
    public ViewQuery staleOk(boolean b) {
        reset();
        staleOk = b ? "ok" : null;
        return this;
    }
    /**
     * Same as staleOk(true) but will also trigger a rebuild of the view index after the results of the view have been retrieved.
     * (since CouchDB 1.1.0)
     * @return
     */
    public ViewQuery staleOkUpdateAfter() {
        reset();
        staleOk = "update_after";
        return this;
    }
    /**
     * View rows are sorted by the key; specifying descending=true will reverse their order. Note that the descending option is applied before any key filtering, so you may need to swap the values of the startkey and endkey options to get the expected results.
     * @param b the descending flag
     * @return the view query for chained calls
     */
    public ViewQuery descending(boolean b) {
        reset();
        descending = b;
        return this;
    }
    /**
     * The skip option should only be used with small values, as skipping a large range of documents this way is inefficient (it scans the index from the startkey and then skips N elements, but still needs to read all the index values to do that). For efficient paging you'll need to use startkey and limit. If you expect to have multiple documents emit identical keys, you'll need to use startkey_docid in addition to startkey to paginate correctly. The reason is that startkey alone will no longer be sufficient to uniquely identify a row.
     * @param i the skip count
     * @return the view query for chained calls
     */
    public ViewQuery skip(int i) {
        reset();
        skip = i;
        return this;
    }
    /**
     * The group option controls whether the reduce function reduces to a set of distinct keys or to a single result row.
     * @param b the group flag
     * @return the view query for chained calls
     */
    public ViewQuery group(boolean b) {
        reset();
        group = b;
        return this;
    }

    public ViewQuery groupLevel(int i) {
        reset();
        groupLevel = i;
        return this;
    }
    /**
     * If a view contains both a map and reduce function, querying that view will by default return the result of the reduce function. The result of the map function only may be retrieved by passing reduce=false as a query parameter.
     * @param b the reduce flag
     * @return the view query for chained calls
     */
    public ViewQuery reduce(boolean b) {
        reset();
        reduce = b;
        return this;
    }
    /**
     * The include_docs option will include the associated document. Although, the user should keep in mind that there is a race condition when using this option. It is possible that between reading the view data and fetching the corresponding document that the document has changed. If you want to alleviate such concerns you should emit an object with a _rev attribute as in emit(key, {"_rev": doc._rev}). This alleviates the race condition but leaves the possiblity that the returned document has been deleted (in which case, it includes the "_deleted": true attribute).
     * @param b the includeDocs flag
     * @return the view query for chained calls
     */
    public ViewQuery includeDocs(boolean b) {
        reset();
        includeDocs = b;
        return this;
    }
    /**
     * The inclusive_end option controls whether the endkey is included in the result. It defaults to true.
     * @param b the inclusiveEnd flag
     * @return the view query for chained calls
     */
    public ViewQuery inclusiveEnd(boolean b) {
        reset();
        inclusiveEnd = b;
        return this;
    }

    /**
     * The update_seq option adds a field to the result indicating the update_seq the view reflects.  It defaults to false.
     * @param b the updateSeq flag
     * @return the view query for chained calls
     */
    public ViewQuery updateSeq(boolean b) {
        reset();
        updateSeq = b;
        return this;
    }

    public ViewQuery queryParam(String name, String value) {
        mQueryParams.put(name, value);
        return this;
    }

    /**
     * Resets internal state so this builder can be used again.
     */
    public void reset() {
        cachedQuery = null;
    }

    public Object getKey() {
        return key;
    }

    public boolean hasMultipleKeys() {
        return keys != null;
    }

    public String getKeysAsJson() {
        if (keys == null) {
            return "{\"keys\":[]}";
        }
        return mGson.toJson(keys);
    }


    public Object getStartKey() {
        return startKey;
    }

    public Object getEndKey() {
        return endKey;
    }

    public String buildQuery() throws RestException {
        if (cachedQuery != null) {
            return cachedQuery;
        }
        cachedQuery = buildQueryURI();
        return cachedQuery;
    }

    public String buildQueryURI() throws RestException {
        String path = buildViewPath();

        HashMap<String, String> params = new HashMap<String, String>();
        if (isNotEmpty(key)) {
            params.put("key", mGson.toJson(key));
        }

        if (isNotEmpty(startKey)) {
            params.put("startkey", mGson.toJson(startKey));
        }

        if (isNotEmpty(endKey)) {
            params.put("endkey", mGson.toJson(endKey));
        }

        if (isNotEmpty(startDocId)) {
            params.put("startkey_docid", "\"" + startDocId + "\"");
        }

        if (isNotEmpty(endDocId)) {
            params.put("endkey_docid", "\"" + endDocId + "\"");
        }

        if (hasValue(limit)) {
            params.put("limit", String.valueOf(limit));
        }

        if (staleOk != null) {
            params.put("stale", staleOk);
        }

        if (descending) {
            params.put("descending", "true");
        }

        if (!inclusiveEnd) {
            params.put("inclusive_end", "false");
        }

        if (!reduce) {
            params.put("reduce", "false");
        }

        if (hasValue(skip)) {
            params.put("skip", String.valueOf(skip));
        }

        if (includeDocs) {
            params.put("include_docs", "true");
        }

        if (group) {
            params.put("group", "true");
        }

        if (hasValue(groupLevel)) {
            params.put("group_level", String.valueOf(groupLevel));
        }

        if (!mQueryParams.isEmpty()) {
            appendQueryParams(params);
        }

        if(updateSeq) {
            params.put("update_seq", "true");
        }
        try {
            return path + "?" + RestUtils.buildQuery(params, RestConnection.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException e) {
            throw new RestException(RestConnection.SC_UNKNOWN, e);
        }
    }

    
    private void appendQueryParams(HashMap<String, String> params) {
        for (Map.Entry<String, String> param : mQueryParams.entrySet()) {
            params.put(param.getKey(), param.getValue());
        }
    }

    private String buildViewPath() {
        assertHasText(viewName, "viewName");

        StringBuilder sb = new StringBuilder();
        if (isNotEmpty(listName)) {
            sb.append(SEPARATOR).append(designDocId).append(SEPARATOR).append("_list").append(SEPARATOR).append(listName).append(SEPARATOR).append(viewName);
        } else if (ALL_DOCS_VIEW_NAME.equals(viewName)) {
            sb.append(SEPARATOR).append(viewName);
        } else {
            assertHasText(designDocId, "designDocId");
            sb.append(SEPARATOR).append(designDocId).append(SEPARATOR).append("_view").append(SEPARATOR).append(viewName);
        }
        return sb.toString();
    }

    private void assertHasText(String s, String fieldName) {
        if (s == null || s.length() == 0) {
            throw new IllegalStateException(String.format("%s must have a value", fieldName));
        }
    }

    private boolean hasValue(int i) {
        return i != NOT_SET;
    }

    private boolean isNotEmpty(Object s) {
        return s != null;
    }
    
    public void setIgnoreNotFound(boolean ignoreNotFound) {
        this.ignoreNotFound = ignoreNotFound;
    }

    public boolean isIgnoreNotFound() {
        return ignoreNotFound;
    }
    

    public static class Keys {

        private final List<?> keys;

        public static Keys of(Collection<?> keys) {
            return new Keys(keys.toArray());
        }

        public static Keys of(Object... keys) {
            return new Keys(keys);
        }

        private Keys(Collection<?> keys) {
            this.keys = new ArrayList<Object>(keys);
        }

        private Keys(Object[] keys) {
            this.keys = Arrays.asList(keys);
        }

        public List<?> getValues() {
            return Collections.unmodifiableList(keys);
        }
    }



}