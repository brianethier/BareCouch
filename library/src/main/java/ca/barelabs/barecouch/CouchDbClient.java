/*
 * Copyright 2014 Brian Ethier
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package ca.barelabs.barecouch;


import java.io.IOException;
import java.util.List;

import ca.barelabs.bareconnection.ContentInputStream;
import ca.barelabs.bareconnection.BackOffPolicy;
import ca.barelabs.bareconnection.ObjectParser;
import ca.barelabs.bareconnection.RestConnection;
import ca.barelabs.bareconnection.RestException;
import ca.barelabs.bareconnection.RestProperties;
import ca.barelabs.bareconnection.RestResponse;
import ca.barelabs.bareconnection.RestUtils;
import ca.barelabs.barecouch.responses.SessionResponse;
import ca.barelabs.barecouch.responses.DatabaseInfo;
import ca.barelabs.barecouch.responses.DocumentResponse;
import ca.barelabs.barecouch.responses.Response;
import ca.barelabs.barecouch.responses.UuidList;
import com.google.gson.reflect.TypeToken;

public class CouchDbClient {

    public static final String REVISION_PARAM = "rev";
    public static final String COUNT_PARAM = "count";
    public static final String ETAG_FIELD = "Etag";
    public static final String ALL_DBS_PATH = "_all_dbs";
    public static final String UUIDS_PATH = "_uuids";
    public static final String SESSION_PATH = "_session";
    public static final String BULK_DOCS_PATH = "_bulk_docs";

    private final RestProperties mProperties;
    private ObjectParser mParser;
    private int mMaxRetryAttempts = RestConnection.DEFAULT_MAX_RETRY_ATTEMPTS;
    private boolean mRetryOnIOException;
    private BackOffPolicy mBackOffPolicy;


    public CouchDbClient(RestProperties properties) {
        mProperties = properties;
    }

    public ObjectParser getParser() {
		return mParser;
	}

	public void setParser(ObjectParser parser) {
		mParser = parser;
	}

    public int getMaxRetryAttempts() {
        return mMaxRetryAttempts;
    }

    public void setMaxRetryAttempts(int maxRetryAttempts) {
        mMaxRetryAttempts = maxRetryAttempts;
    }
    
    public boolean isRetryOnIOException() {
        return mRetryOnIOException;
    }
    
    public void setRetryOnIOException(boolean retryOnIOException) {
        mRetryOnIOException = retryOnIOException;
    }
    
    public BackOffPolicy getBackOffPolicy() {
        return mBackOffPolicy;
    }
    
    public void setBackOffPolicy(BackOffPolicy backOffPolicy) {
        mBackOffPolicy = backOffPolicy;
    }

	public UuidList getUuidList(int count) throws IOException {
        return executeUuidList(count).parseAs(UuidList.class);
    }

    public SessionResponse getSession() throws IOException {
        return executeSessionGet().parseAs(SessionResponse.class);
    }

    public <D> D getSession(Class<D> clss) throws IOException {
        return executeSessionGet().parseAs(clss);
    }
    
    public List<String> getAllDatabases() throws IOException {
        return executeAllDatabasesGet().parseAs(new TypeToken<List<String>>(){}.getType());
    }
    
    public DatabaseInfo getDatabaseInfo(String database) throws IOException {
        return executeDatabaseGet(database).parseAs(DatabaseInfo.class);
    }
    
    public <R> R getDatabaseInfo(String database, Class<R> clss) throws IOException {
        return executeDatabaseGet(database).parseAs(clss);
    }
    
    public boolean exists(String database) throws IOException {
		try {
			executeDatabaseHead(database).parse();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't exists
			if (e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return false;
			}
			throw e;
		}
    }
    
    public boolean createDatabase(String database) throws IOException {
    	try {
			return executeDatabasePut(database).parseAs(Response.class).isOk();
		} catch (RestException e) {
			// CouchDb returns a 412 Precondition Failed if database already exists
			if (e.getStatusCode() == RestConnection.SC_PRECON_FAILED) {
				return false;
			}
			throw e;
		}
    }

    public boolean deleteDatabase(String database) throws IOException {
		try {
			return executeDatabaseDelete(database).parseAs(Response.class).isOk();
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't exist
			if (e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return false;
			}
			throw e;
		}
    }

    public boolean contains(String database, String docId) throws IOException {
		try {
			executeDocumentHead(database, docId).parse();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if (e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return false;
			}
			throw e;
		}
    }

    public boolean containsAsLatest(String database, String docId, String revId) throws IOException {
        try {
        	RestResponse response = executeDocumentHead(database, docId);
        	response.parse();
            String etag = response.getConnection().getHeaderField(ETAG_FIELD);
            return revId != null && etag != null && revId.equals(etag);
        } catch (RestException e) {
            // CouchDb returns a 404 Not Found if database doesn't contain document
            if (e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    public <D> D get(String database, String docId, Class<D> documentClss) throws IOException {
    	try {
        	return executeDocumentGet(database, docId).parseAs(documentClss);
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if (e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return null;
			}
			throw e;
		}
    }

    public <D> D find(String database, String docId, Class<D> documentClss) throws IOException {
		return executeDocumentGet(database, docId).parseAs(documentClss);
    }

    public DocumentResponse create(String database, Object document) throws IOException {
        return create(database, document, DocumentResponse.class);
    }

    public <D> D create(String database, Object document, Class<D> responseClss) throws IOException {
    	String docId = DocumentUtils.getId(document);
        return create(database, docId, document, responseClss);
    }

    public DocumentResponse create(String database, String docId, Object document) throws IOException {
        return create(database, docId, document, DocumentResponse.class);
    }

    public <D> D create(String database, String docId, Object document, Class<D> responseClss) throws IOException {
    	RestResponse response = docId == null ? executeDocumentPost(database, document) : executeDocumentPut(database, docId, document);
	    D documentResponse = response.parseAs(responseClss);
	    DocumentUtils.setId(document, DocumentUtils.getId(documentResponse));
	    DocumentUtils.setRev(document, DocumentUtils.getRev(documentResponse));
	    return documentResponse;
    }

    public DocumentResponse update(String database, Object document) throws IOException {
        return update(database, DocumentUtils.getId(document), document, DocumentResponse.class);
    }

    public <D> D update(String database, Object document, Class<D> responseClss) throws IOException {
        return update(database, DocumentUtils.getId(document), document, responseClss);
    }

    public DocumentResponse update(String database, String docId, Object document) throws IOException {
        return update(database, docId, document, DocumentResponse.class);
    }

    public <D> D update(String database, String docId, Object document, Class<D> responseClss) throws IOException {
    	RestResponse response = executeDocumentPut(database, docId, document);
	    D documentResponse = response.parseAs(responseClss);
	    DocumentUtils.setRev(document, DocumentUtils.getRev(documentResponse));
	    return documentResponse;
    }

    public DocumentResponse delete(String database, Object document) throws IOException {
        return delete(database, document, DocumentResponse.class);
    }

    public <D> D delete(String database, Object document, Class<D> responseClss) throws IOException {
    	D documentResponse = delete(database, DocumentUtils.getId(document), DocumentUtils.getRev(document), responseClss);
	    DocumentUtils.setRev(document, DocumentUtils.getRev(documentResponse));
        return documentResponse;
    }
    
    public DocumentResponse delete(String database, String docId, String rev) throws IOException {
        return delete(database, docId, rev, DocumentResponse.class);
    }
    
    public <D> D delete(String database, String docId, String rev, Class<D> responseClss) throws IOException {
    	RestResponse response = executeDocumentDelete(database, docId, rev);
	    return response.parseAs(responseClss);
    }
    
    public ContentInputStream getAttachment(String database, String docId, String attachmentName) throws IOException {
        return getAttachment(database, docId, null, attachmentName);
    }
    
    public ContentInputStream getAttachment(String database, String docId, String rev, String attachmentName) throws IOException {
        RestResponse response = executeGetAttachment(database, docId, rev, attachmentName);
        String contentType = response.getConnection().getContentType();
        int contentLength = response.getConnection().getContentLength();
        return new ContentInputStream(response.getContent(), contentType, contentLength);
    }
    
    public DocumentResponse createAttachment(String database, String docId, String rev, String attachmentName, ContentInputStream in) throws IOException {
        return createAttachment(database, docId, rev, attachmentName, in, DocumentResponse.class);
    }
    
    public <D> D createAttachment(String database, String docId, String rev, String attachmentName, ContentInputStream in, Class<D> responseClss) throws IOException {
    	RestResponse response = executeCreateAttachment(database, docId, rev, attachmentName, in);
	    return response.parseAs(responseClss);
    }

    public BulkResult bulkUpdate(String database, Object request) throws IOException {
        RestResponse response = executeBulkUpdate(database, request);
        return new BulkResult(response);
    }
    
    public ViewResult queryView(String database, ViewQuery query) throws IOException {
    	RestResponse response = executeViewQuery(database, query);
        return new ViewResult(query, response);
    }
    
    public StreamingViewResult queryForStreamingView(String database, ViewQuery query) throws IOException {
    	RestResponse response = executeViewQuery(database, query);
        return new StreamingViewResult(query, response);
    }
    
    public ChangesResult queryChanges(String database, ChangesQuery query) throws IOException {
    	RestResponse response = executeChangesQuery(database, query);
        return new ChangesResult(query, response);
    }
    
    public StreamingChangesResult queryForStreamingChanges(String database, ChangesQuery query) throws IOException {
    	RestResponse response = executeChangesQuery(database, query);
        return new StreamingChangesResult(query, response);
    }
    
	public RestResponse executeUuidList(int count) throws IOException {
        RestConnection connection = newConnectionBuilder(UUIDS_PATH)
            .param(COUNT_PARAM, String.valueOf(count))
            .build();
        return connection.get();
    }

    public RestResponse executeSessionGet() throws IOException {
        return executeDatabaseGet(SESSION_PATH);
    }
    
    public RestResponse executeAllDatabasesGet() throws IOException {
        return executeDatabaseGet(ALL_DBS_PATH);
    }

    public RestResponse executeDatabaseHead(String database) throws IOException {
        ensureDatabase(database);
		RestConnection connection = createConnection(database);
		return connection.head();
    }
    
    public RestResponse executeDatabaseGet(String database) throws IOException {
        ensureDatabase(database);
        RestConnection connection = createConnection(database);
        return connection.get();
    }
    
    public RestResponse executeDatabasePut(String database) throws IOException {
        ensureDatabase(database);
        RestConnection connection = createConnection(database);
        return connection.put();
    }
    
    public RestResponse executeDatabaseDelete(String database) throws IOException {
        ensureDatabase(database);
        RestConnection connection = createConnection(database);
        return connection.delete();
    }

    public RestResponse executeDocumentHead(String database, String docId) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
		RestConnection connection = createConnection(database, docId);
		return connection.head();
    }

    public RestResponse executeDocumentGet(String database, String docId) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
		RestConnection connection = createConnection(database, docId);
		return connection.get();
    }

    public RestResponse executeDocumentPut(String database, String docId, Object document) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
		RestConnection connection = createConnection(database, docId);
		return connection.put(document);
    }

    public RestResponse executeDocumentPost(String database, Object document) throws IOException {
        ensureDatabase(database);
		RestConnection connection = createConnection(database);
		return connection.post(document);
    }

    public RestResponse executeDocumentDelete(String database, String docId, String docRev) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
        ensureDocumentRev(docRev);
        RestConnection connection = newConnectionBuilder(database, docId)
            .param(REVISION_PARAM, docRev)
            .build();
		return connection.delete();
    }

    public RestResponse executeGetAttachment(String database, String docId, String docRev, String attachmentName) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
        ensureAttachmentName(attachmentName);
        RestConnection.Builder builder = newConnectionBuilder(database, docId, attachmentName);
        if (docRev != null) {
            builder.param(REVISION_PARAM, docRev);
        }
        RestConnection connection = builder.build();
        return connection.get();
    }
      

    public RestResponse executeCreateAttachment(String database, String docId, String docRev, String attachmentName, Object object) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
        ensureDocumentRev(docRev);
        ensureAttachmentName(attachmentName);
        RestConnection connection = newConnectionBuilder(database, docId, attachmentName)
            .param(REVISION_PARAM, docRev)
            .build();
        return connection.put(object);
    }

    public RestResponse executeBulkUpdate(String database, Object object) throws IOException {
        ensureDatabase(database);
		RestConnection connection = createConnection(database, BULK_DOCS_PATH);
        if (object instanceof List<?>) {
            DocumentBulkRequest request = new DocumentBulkRequest();
            request.setDocs((List<?>) object);
        	return connection.post(request);
        } else {
        	return connection.post(object);
        }
    }

    public RestResponse executeViewQuery(String database, ViewQuery query) throws IOException {
        ensureDatabase(database);
		RestConnection connection = createConnection(database + query.buildQuery());
        if (query.hasMultipleKeys()) {
        	return connection.post(query.getKeysAsJson());
        } else {
        	return connection.get();
        }
    }

    public RestResponse executeChangesQuery(String database, ChangesQuery query) throws IOException {
        ensureDatabase(database);
		RestConnection connection = createConnection(database + query.buildQuery());
    	return connection.get();
    }
    
    private RestConnection createConnection(String... paths) throws IOException {
        return newConnectionBuilder(paths).build();
    }
    
    private RestConnection.Builder newConnectionBuilder(String... paths) throws IOException {
    	return new RestConnection.Builder()
            .properties(mProperties)
            .parser(mParser)
            .maxRetryAttempts(mMaxRetryAttempts)
            .retryOnIOException(mRetryOnIOException)
            .backOffPolicy(mBackOffPolicy)
            .path(RestUtils.toPath(paths));
    }
    
    private void ensureDatabase(String database) throws IOException {
        if (database == null || database.isEmpty()) {
            throw new IllegalArgumentException("No database was provided for this operation!");
        }
    }
    
    private void ensureDocumentId(String docId) throws IOException {
        if (docId == null) {
            throw new IllegalArgumentException("The document must have a valid id.");
        }
    }
    
    private void ensureDocumentRev(String docRev) throws IOException {
        if (docRev == null) {
            throw new IllegalArgumentException("The document must have a valid revision.");
        }
    }
    
    private void ensureAttachmentName(String attachmentName) throws IOException {
        if (attachmentName == null) {
            throw new IllegalArgumentException("The attachment must have a valid name.");
        }
    }
    
    
    public static final class Builder {

        private ObjectParser mParser;
        private int mMaxRetryAttempts = RestConnection.DEFAULT_MAX_RETRY_ATTEMPTS;
        private boolean mRetryOnIOException;
        private BackOffPolicy mBackOffPolicy;
        private RestProperties.Builder mPropertiesBuilder = new RestProperties.Builder();

        
        public Builder parser(ObjectParser parser) {
            mParser = parser;
            return this;    
        }
        
        public Builder maxRetryAttempts(int maxRetryAttempts) {
            mMaxRetryAttempts = maxRetryAttempts;
            return this;    
        }
        
        public Builder retryOnIOException(boolean retryOnIOException) {
            mRetryOnIOException = retryOnIOException;
            return this;    
        }
        
        public Builder backOffPolicy(BackOffPolicy backOffPolicy) {
            mBackOffPolicy = backOffPolicy;
            return this;    
        }
        
        public Builder url(String url) {
            mPropertiesBuilder.url(url);
            return this;    
        }
        
        public Builder path(String path) {
            mPropertiesBuilder.path(path);
            return this;    
        }
        
        public Builder username(String username) {
            mPropertiesBuilder.username(username);
            return this;    
        }
        
        public Builder password(String password) {
            mPropertiesBuilder.password(password);
            return this;    
        }
        
        public Builder connectTimeout(int connectTimeout) {
            mPropertiesBuilder.connectTimeout(connectTimeout);
            return this;    
        }
        
        public Builder readTimeout(int readTimeout) {
            mPropertiesBuilder.readTimeout(readTimeout);
            return this;    
        }
        
        public Builder followRedirects(boolean followRedirects) {
            mPropertiesBuilder.followRedirects(followRedirects);
            return this;    
        }
        
        public Builder properties(RestProperties properties) {
            if (properties != null) {
                mPropertiesBuilder
                    .url(properties.getUrl())
                    .path(properties.getPath())
                    .username(properties.getUsername())
                    .password(properties.getPassword())
                    .connectTimeout(properties.getConnectTimeout())
                    .readTimeout(properties.getReadTimeout());
            }
            return this;    
        }
        
        public CouchDbClient build() {
            RestProperties properties = mPropertiesBuilder.build();
        	CouchDbClient client = new CouchDbClient(properties);
        	client.mParser = mParser;
        	client.mMaxRetryAttempts = mMaxRetryAttempts;
        	client.mBackOffPolicy = mBackOffPolicy;
        	client.mRetryOnIOException = mRetryOnIOException;
        	return client;
        }
    }
}
