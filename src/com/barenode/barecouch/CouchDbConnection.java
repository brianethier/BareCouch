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
package com.barenode.barecouch;


import java.io.BufferedInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.barenode.bareconnection.RestConnection;
import com.barenode.bareconnection.RestException;
import com.barenode.bareconnection.RestProperties;

public class CouchDbConnection {

    public static final String REVISION_PARAM = "rev";
    public static final String COUNT_PARAM = "count";
    public static final String ETAG_FIELD = "Etag";
    public static final String ALL_DBS_PATH = "_all_dbs";
    public static final String UUIDS_PATH = "_uuids";
    public static final String SESSION_PATH = "_session";

    private final RestProperties mProperties;
    private String mDatabase;


    public CouchDbConnection(RestProperties properties) {
        mProperties = properties;
    }

    public CouchDbConnection(RestProperties properties, String database) {
    	mProperties = properties;
    	mDatabase = database;
    }

    
    /* DATABASE API */
    
    public List<String> getAllDatabases() throws RestException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(ALL_DBS_PATH)
            .build();
        return connection.getReturnList(String.class);
    }
    
    public List<String> getUUIDs(int count) throws RestException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(UUIDS_PATH)
            .param(COUNT_PARAM, String.valueOf(count))
            .build();
        return connection.get(UUIDList.class).uuids;
    }

    public <D> D getSession(Class<D> clss) throws RestException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(SESSION_PATH)
            .build();
        return connection.get(clss);
    }
    
    public boolean createDatabase() throws RestException {
        ensureDatabase();
    	try {
    		RestConnection connection = createConnection(null);
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

    public Response deleteDatabase() throws RestException {
        return deleteDatabase(Response.class);
    }

    public <R> R deleteDatabase(Class<R> clss) throws RestException {
        ensureDatabase();
		try {
			RestConnection connection = createConnection(null);
			return connection.delete(clss);
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't exist
			if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return null;
			}
			throw e;
		}
    }
    
    public DatabaseInfo getDatabaseInfo() throws RestException {
        ensureDatabase();
		RestConnection connection = createConnection(null);
		return connection.get(DatabaseInfo.class);
    }
    
    public boolean exists() throws RestException {
        ensureDatabase();
		try {
    		RestConnection connection = createConnection(null);
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
    
    /* RETRIEVE API */

    public boolean contains(String docId) throws RestException {
        ensureDatabase();
		try {
    		RestConnection connection = createConnection(docId);
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

    public boolean containsAsLatest(String docId, String revId) throws RestException {
        ensureDatabase();
        try {
            RestConnection connection = createConnection(docId);
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

    public <D> D get(Class<D> clss, String docId) throws RestException {
        ensureDatabase();
    	try {
    		RestConnection connection = createConnection(docId);
			return connection.get(clss);
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if(e.getStatusCode() == RestConnection.SC_NOT_FOUND) {
				return null;
			}
			throw e;
		}
    }

    public <D> D find(Class<D> clss, String docId) throws RestException {
        ensureDatabase();
		RestConnection connection = createConnection(docId);
		return connection.get(clss);
    }
    
    /* CREATE API */

    public void create(Object document) throws RestException {
        ensureDatabase();
        RestConnection connection = createConnection(null);
        Response response = connection.post(Response.class, document);
        DocumentUtils.setId(document, response.getId());
        DocumentUtils.setRev(document, response.getRev());
    }

    public void create(String docId, InputStream in) throws RestException {
        ensureDatabase();
        ensureDocumentId(docId);
        RestConnection connection = createConnection(docId);
        connection.put(Response.class, in);
    }
    
    /* UPDATE API */

    public void update(Object document) throws RestException {
        ensureDatabase();
        ensureDocumentId(DocumentUtils.getId(document));
        RestConnection connection = createConnection(DocumentUtils.getId(document));
        Response response = connection.put(Response.class, document);
		DocumentUtils.setRev(document, response.getRev());
    }

    public void update(String docId, InputStream in) throws RestException {
        ensureDatabase();
        ensureDocumentId(docId);
        RestConnection connection = createConnection(docId);
        connection.put(Response.class, in);
    }
    
    /* DELETE API */

    public String delete(Object document) throws RestException {
        ensureDatabase();
        ensureDocumentId(DocumentUtils.getId(document));
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(REVISION_PARAM, DocumentUtils.getRev(document));
        RestConnection connection = createConnection(DocumentUtils.getId(document), params);
        Response response = connection.delete(Response.class);
        return response.getRev();
    }
    
    /* QUERY API */
    
    public <T> List<T> queryView(ViewQuery query, Class<T> clss) throws RestException {
        ViewResult result = queryView(query);
        Iterator<ViewResult.Row> iterator = result.iterator();
        ArrayList<T> list = new ArrayList<T>();
        while(iterator.hasNext()) {
            ViewResult.Row row = iterator.next();
            list.add(query.isIncludeDocs() ? row.getDocAsObject(clss) : row.getValueAsObject(clss));
        }
        return list;
    }
    
    public ViewResult queryView(ViewQuery query) throws RestException {
        ensureDatabase();
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(mDatabase + query.buildQuery())
            .build();
        try {
            if(query.hasMultipleKeys()) {
                String keysAsJson = query.getKeysAsJson();
                Reader reader = new InputStreamReader(new BufferedInputStream(connection.post(InputStream.class, keysAsJson)), RestConnection.DEFAULT_CHARSET);
                return new ViewResult(reader);
            } else {
                Reader reader = new InputStreamReader(new BufferedInputStream(connection.get(InputStream.class)), RestConnection.DEFAULT_CHARSET);
                return new ViewResult(reader);
            }
        } catch (UnsupportedEncodingException e) {
            throw new RestException(RestConnection.SC_UNKNOWN, e);
        }
    }
    
    public StreamingViewResult queryForStreamingView(ViewQuery query) throws RestException {
        ensureDatabase();
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(mDatabase + query.buildQuery())
            .build();
        try {
            if(query.hasMultipleKeys()) {
                String keysAsJson = query.getKeysAsJson();
                Reader reader = new InputStreamReader(new BufferedInputStream(connection.post(InputStream.class, keysAsJson)), RestConnection.DEFAULT_CHARSET);
                return new StreamingViewResult(connection, reader);
            } else {
                Reader reader = new InputStreamReader(new BufferedInputStream(connection.get(InputStream.class)), RestConnection.DEFAULT_CHARSET);
                return new StreamingViewResult(connection, reader);
            }
        } catch (UnsupportedEncodingException e) {
            throw new RestException(RestConnection.SC_UNKNOWN, e);
        }
    }
    
//    public <T> ViewIterator<T> queryView(String path, Class<T> clss, String key, String key2) throws RestException {
//        ensureDatabase();
//    	HashMap<String, String> map = new HashMap<String,String>();
//    	map.put("key", key);
//		RestConnection connection = new RestConnection.Builder()
//			.properties(mProperties)
//			.path(mDatabase + path, key == null ? null : map)
//			.build();
////		ViewResult result = new ViewResult(connection, key2);
////    	return result.iterator(clss);
//    	return null;
//    }

//    public ViewIterator query(String database, String viewName) throws RestException {
//        RestConnection connection = new RestConnection(SERVER + "/" + database + viewName);
//        connection.setAuthorization(mUsername, mPassword);
//        return new ViewIterator(connection);
//    }
//
//    public <T> List<T> query(String database, String viewName, Class<T> clss) throws RestException {
//        ArrayList<T> list = new ArrayList<T>();
//        ViewIterator iterator = query(database, viewName);
//        while(iterator.hasNext())
//            list.add(iterator.next(clss));
//        iterator.close();
//        return list;
//    }
    
    private RestConnection createConnection(String path) throws RestException {
        return createConnection(path, null);
    }
    
    private RestConnection createConnection(String path, HashMap<String, String> params) throws RestException {
        return new RestConnection.Builder()
            .properties(mProperties)
            .path(path == null ? mDatabase : mDatabase + RestConnection.PATH_SEPARATOR + path)
            .params(params)
            .build();
    }
    
    private void ensureDatabase() throws RestException {
        if(mDatabase == null || mDatabase.isEmpty()) {
            throw new RestException(RestConnection.SC_UNKNOWN, "No database was provided for this operation!");
        }
    }
    
    private void ensureDocumentId(String docId) throws RestException {
        if(docId == null) {
            throw new RestException(RestConnection.SC_UNKNOWN, "You must provide a valid docId!");
        }
    }
    
    
    public static class UUIDList {
    	private List<String> uuids;
    }
}
