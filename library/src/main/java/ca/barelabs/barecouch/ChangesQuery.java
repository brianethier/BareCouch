package ca.barelabs.barecouch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import ca.barelabs.bareconnection.RestConnection;
import ca.barelabs.bareconnection.RestUtils;

import com.google.gson.Gson;

public class ChangesQuery {

    public final static String CHANGES_PATH = "/_changes";
    public final static String PARAM_DOC_IDS = "doc_ids";
    public final static String PARAM_CONFLICTS = "conflicts";
    public final static String PARAM_DESCENDING = "descending";
    public final static String PARAM_FEED = "feed";
    public final static String PARAM_FILTER = "filter";
    public final static String PARAM_HEARTBEAT = "heartbeat";
    public final static String PARAM_INCLUDE_DOCS = "include_docs";	
    public final static String PARAM_ATTACHMENTS = "attachments";
    public final static String PARAM_ATT_ENCODING_INFO = "att_encoding_info";
    public final static String PARAM_LAST_EVENT_ID = "last-event-id";
    public final static String PARAM_LIMIT = "limit";
    public final static String PARAM_SINCE = "since";
    public final static String PARAM_STYLE = "style";
    public final static String PARAM_TIMEOUT = "timeout";
    public final static String PARAM_VIEW = "view";
    
    private final static int NOT_SET = -1;

    private final Gson mGson;
    private String[] mDocIds;
    private boolean mConflicts;
    private boolean mDescending;
    private String mFeed;
    private String mFilter;
    private int mHeartbeat;
    private boolean mIncludeDocs;
    private boolean mAttachments;
    private boolean mAttEncodingInfo;
    private int mLastEventId = NOT_SET;
    private int mLimit = NOT_SET;
    private String mSince;
    private String mStyle;
    private int mTimeout = NOT_SET;
    private String mView;

    
    /**
     * Provide your own Gson parser to use when serializing keys when building query.
     * @param gson
     */
    public ChangesQuery(Gson gson) {
        mGson = gson;
    }
    
    public ChangesQuery() {
        this(new Gson());
    }
    
    public String[] getDocIds() {
		return mDocIds;
	}

	public ChangesQuery docIds(String[] docIds) {
		mDocIds = docIds;
        return this;
	}

	public boolean isConflicts() {
		return mConflicts;
	}
	
	public ChangesQuery conflicts(boolean conflicts) {
		mConflicts = conflicts;
        return this;
	}

	public boolean isDescending() {
		return mDescending;
	}

	public ChangesQuery descending(boolean descending) {
		mDescending = descending;
        return this;
	}

	public String getFeed() {
		return mFeed;
	}

	public ChangesQuery feed(String feed) {
		mFeed = feed;
        return this;
	}

	public String getFilter() {
		return mFilter;
	}

	public ChangesQuery filter(String filter) {
		mFilter = filter;
        return this;
	}

	public int getHeartbeat() {
		return mHeartbeat;
	}

	public ChangesQuery heartbeat(int heartbeat) {
		mHeartbeat = heartbeat;
        return this;
	}

	public boolean isIncludeDocs() {
		return mIncludeDocs;
	}

	public ChangesQuery includeDocs(boolean includeDocs) {
		mIncludeDocs = includeDocs;
        return this;
	}

	public boolean isAttachments() {
		return mAttachments;
	}

	public ChangesQuery attachments(boolean attachments) {
		mAttachments = attachments;
        return this;
	}

	public boolean isAttEncodingInfo() {
		return mAttEncodingInfo;
	}

	public ChangesQuery attachmentsEncodingInfo(boolean attEncodingInfo) {
		mAttEncodingInfo = attEncodingInfo;
        return this;
	}

	public int getLastEventId() {
		return mLastEventId;
	}

	public ChangesQuery lastEventId(int lastEventId) {
		mLastEventId = lastEventId;
        return this;
	}

	public int getLimit() {
		return mLimit;
	}

	public ChangesQuery limit(int limit) {
		mLimit = limit;
        return this;
	}

	public String getSince() {
		return mSince;
	}

	public ChangesQuery since(String since) {
		mSince = since;
        return this;
	}

    public ChangesQuery since(long since) {
        mSince = String.valueOf(since);
        return this;
    }

	public String getStyle() {
		return mStyle;
	}

	public ChangesQuery style(String style) {
		mStyle = style;
        return this;
	}

	public int getTimeout() {
		return mTimeout;
	}

	public ChangesQuery timeout(int timeout) {
		mTimeout = timeout;
        return this;
	}

	public String getView() {
		return mView;
	}

	public ChangesQuery view(String view) {
		mView = view;
        return this;
	}

	public String buildQuery() throws IOException {
        Map<String, String> params = createParams();
        return params.isEmpty() ? CHANGES_PATH :
        	CHANGES_PATH + "?" + RestUtils.toQuery(params, RestConnection.DEFAULT_CHARSET);
    }

    private Map<String, String> createParams() {
        HashMap<String, String> params = new HashMap<String, String>();
        if (mDocIds != null) {
            params.put(PARAM_DOC_IDS, mGson.toJson(mDocIds));
        }
        if (mConflicts) {
            params.put(PARAM_CONFLICTS, String.valueOf(mConflicts));
        }
        if (mDescending) {
            params.put(PARAM_DESCENDING, String.valueOf(mDescending));
        }
        if (mFeed != null) {
            params.put(PARAM_FEED, mFeed);
        }
        if (mFilter != null) {
            params.put(PARAM_FILTER, mFilter);
        }
        if (mHeartbeat != NOT_SET) {
            params.put(PARAM_HEARTBEAT, String.valueOf(mHeartbeat));
        }
        if (mIncludeDocs) {
            params.put(PARAM_INCLUDE_DOCS, String.valueOf(mIncludeDocs));
        }
        if (mAttachments) {
            params.put(PARAM_ATTACHMENTS, String.valueOf(mAttachments));
        }
        if (mAttEncodingInfo) {
            params.put(PARAM_ATT_ENCODING_INFO, String.valueOf(mAttEncodingInfo));
        }
        if (mLastEventId != NOT_SET) {
            params.put(PARAM_LAST_EVENT_ID, String.valueOf(mLastEventId));
        }
        if (mLimit != NOT_SET) {
            params.put(PARAM_LIMIT, String.valueOf(mLimit));
        }
        if (mSince != null) {
            params.put(PARAM_SINCE, mSince);
        }
        if (mStyle != null) {
            params.put(PARAM_STYLE, mStyle);
        }
        if (mTimeout != NOT_SET) {
            params.put(PARAM_TIMEOUT, String.valueOf(mTimeout));
        }
        if (mView != null) {
            params.put(PARAM_VIEW, mView);
        }
        return params;
    }
}
