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


import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import ca.barelabs.bareconnection.RestConnection;
import ca.barelabs.bareconnection.RestException;
import ca.barelabs.bareconnection.RestProperties;

public class CouchDbClient {

    public static final String REVISION_PARAM = "rev";
    public static final String COUNT_PARAM = "count";
    public static final String ETAG_FIELD = "Etag";
    public static final String ALL_DBS_PATH = "_all_dbs";
    public static final String UUIDS_PATH = "_uuids";
    public static final String SESSION_PATH = "_session";

    private final RestProperties mProperties;


    public CouchDbClient(RestProperties properties) {
        mProperties = properties;
    }
    
    public List<String> getUUIDs(int count) throws IOException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(UUIDS_PATH)
            .param(COUNT_PARAM, String.valueOf(count))
            .build();
        return connection.get(UUIDList.class).uuids;
    }

    public AuthSession getSession() throws IOException {
        return getSession(AuthSession.class);
    }

    public <D> D getSession(Class<D> clss) throws IOException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(SESSION_PATH)
            .build();
        return connection.get(clss);
    }
    
    public List<String> getAllDatabases() throws IOException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(ALL_DBS_PATH)
            .build();
        return connection.getReturnList(String.class);
    }
    
    public boolean createDatabase(String database) throws IOException {
        ensureDatabase(database);
    	try {
    		RestConnection connection = createConnection(database);
			connection.put();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 412 Precondition Failed if database already exists
			if(e.getStatusCode() == RestConnection.SC_PRECON_FAILED) {
				return false;
			}
			throw e;
		}
    }

    public Response deleteDatabase(String database) throws IOException {
        return deleteDatabase(database, Response.class);
    }

    public <R> R deleteDatabase(String database, Class<R> clss) throws IOException {
        ensureDatabase(database);
		try {
			RestConnection connection = createConnection(database);
			return connection.delete(clss);
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't exist
			if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return null;
			}
			throw e;
		}
    }
    
    public DatabaseInfo getDatabaseInfo(String database) throws IOException {
		return getDatabaseInfo(database, DatabaseInfo.class);
    }
    
    public <R> R getDatabaseInfo(String database, Class<R> clss) throws IOException {
        ensureDatabase(database);
        RestConnection connection = createConnection(database);
        return connection.get(clss);
    }
    
    public boolean exists(String database) throws IOException {
        ensureDatabase(database);
		try {
    		RestConnection connection = createConnection(database);
			connection.head();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't exists
			if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return false;
			}
			throw e;
		}
    }

    public boolean contains(String database, String docId) throws IOException {
        ensureDatabase(database);
		try {
    		RestConnection connection = createConnection(database, docId);
			connection.head();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return false;
			}
			throw e;
		}
    }

    public boolean containsAsLatest(String database, String docId, String revId) throws IOException {
        ensureDatabase(database);
        try {
            RestConnection connection = createConnection(database, docId);
            connection.head();
            String etag = connection.getConnection().getHeaderField(ETAG_FIELD);
            return revId != null && etag != null && revId.equals(etag);
        } catch (RestException e) {
            // CouchDb returns a 404 Not Found if database doesn't contain document
            if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
                return false;
            }
            throw e;
        }
    }

    public <D> D get(String database, String docId, Class<D> documentClss) throws IOException {
        ensureDatabase(database);
    	try {
    		RestConnection connection = createConnection(database, docId);
			return connection.get(documentClss);
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return null;
			}
			throw e;
		}
    }

    public <D> D find(String database, String docId, Class<D> documentClss) throws IOException {
        ensureDatabase(database);
		RestConnection connection = createConnection(database, docId);
		return connection.get(documentClss);
    }

    public Response create(String database, Object document) throws IOException {
        Response response = create(database, document, Response.class);
        DocumentUtils.setId(document, response.getId());
        DocumentUtils.setRev(document, response.getRev());
        return response;
    }

    public <D> D create(String database, Object document, Class<D> responseClss) throws IOException {
        ensureDatabase(database);
        RestConnection connection = createConnection(database);
        return connection.post(responseClss, document);
    }

    public Response create(String database, String docId, InputStream in) throws IOException {
        return create(database, docId, in, Response.class);
    }

    public <D> D create(String database, String docId, InputStream in, Class<D> responseClss) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
        RestConnection connection = createConnection(database, docId);
        return connection.put(responseClss, in);
    }

    public Response update(String database, Object document) throws IOException {
        Response response = update(database, document, Response.class);
		DocumentUtils.setRev(document, response.getRev());
		return response;
    }

    public <D> D update(String database, Object document, Class<D> responseClss) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(DocumentUtils.getId(document));
        RestConnection connection = createConnection(database, DocumentUtils.getId(document));
        return connection.put(responseClss, document);
    }

    public Response update(String database, String docId, InputStream in) throws IOException {
        return update(database, docId, in, Response.class);
    }

    public <D> D update(String database, String docId, InputStream in, Class<D> responseClss) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
        RestConnection connection = createConnection(database, docId);
        return connection.put(responseClss, in);
    }

    public Response delete(String database, Object document) throws IOException {
        ensureDatabase(database);
        Response response = delete(database, DocumentUtils.getId(document), DocumentUtils.getRev(document), Response.class);
        DocumentUtils.setRev(document, response.getRev());
        return response;
    }

    public <D> D delete(String database, Object document, Class<D> responseClss) throws IOException {
        return delete(database, DocumentUtils.getId(document), DocumentUtils.getRev(document), responseClss);
    }
    
    public Response delete(String database, String docId, String rev) throws IOException {
        return delete(database, docId, rev, Response.class);
    }
    
    public <D> D delete(String database, String docId, String rev, Class<D> responseClss) throws IOException {
        ensureDatabase(database);
        ensureDocumentId(docId);
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(REVISION_PARAM, rev);
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(database + RestConnection.PATH_SEPARATOR + docId)
            .params(params)
            .build();
        return connection.delete(responseClss);
    }
    
    
    public <T> List<T> queryView(String database, ViewQuery query, Class<T> clss) throws IOException {
        ViewResult result = queryView(database, query);
        Iterator<ViewResult.Row> iterator = result.iterator();
        ArrayList<T> list = new ArrayList<T>();
        while(iterator.hasNext()) {
            ViewResult.Row row = iterator.next();
            list.add(query.isIncludeDocs() ? row.getDocAsObject(clss) : row.getValueAsObject(clss));
        }
        return list;
    }
    
    public ViewResult queryView(String database, ViewQuery query) throws IOException {
        ensureDatabase(database);
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(database + query.buildQuery())
            .build();
        if(query.hasMultipleKeys()) {
            String keysAsJson = query.getKeysAsJson();
            Reader reader = new InputStreamReader(new BufferedInputStream(connection.post(InputStream.class, keysAsJson)), RestConnection.DEFAULT_CHARSET);
            return new ViewResult(reader);
        } else {
            Reader reader = new InputStreamReader(new BufferedInputStream(connection.get(InputStream.class)), RestConnection.DEFAULT_CHARSET);
            return new ViewResult(reader);
        }
    }
    
    public StreamingViewResult queryForStreamingView(String database, ViewQuery query) throws IOException {
        ensureDatabase(database);
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(database + query.buildQuery())
            .build();
        if(query.hasMultipleKeys()) {
            String keysAsJson = query.getKeysAsJson();
            Reader reader = new InputStreamReader(new BufferedInputStream(connection.post(InputStream.class, keysAsJson)), RestConnection.DEFAULT_CHARSET);
            return new StreamingViewResult(connection, reader);
        } else {
            Reader reader = new InputStreamReader(new BufferedInputStream(connection.get(InputStream.class)), RestConnection.DEFAULT_CHARSET);
            return new StreamingViewResult(connection, reader);
        }
    }
    
    private RestConnection createConnection(String database) throws IOException {
        return createConnection(database, null);
    }
    
    private RestConnection createConnection(String database, String docId) throws IOException {
        return new RestConnection.Builder()
            .properties(mProperties)
            .path(docId == null ? database : database + RestConnection.PATH_SEPARATOR + docId)
            .build();
    }
    
    private void ensureDatabase(String database) throws IOException {
        if(database == null || database.isEmpty()) {
            throw new IllegalArgumentException("No database was provided for this operation!");
        }
    }
    
    private void ensureDocumentId(String docId) throws IOException {
        if(docId == null) {
            throw new IllegalArgumentException("You must provide a valid docId!");
        }
    }
    
    
    public static class UUIDList {
    	private List<String> uuids;
    }
}
