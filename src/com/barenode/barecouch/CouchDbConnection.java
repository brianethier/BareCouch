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
    public static final String ETAG_FIELD = "Etag";
    public static final String SEPARATOR = "/";
    public static final String ALL_DBS_URL = "/_all_dbs";
    public static final String SESSION_URL = "/_session";

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
            .path(ALL_DBS_URL)
            .build();
        return connection.getList(String.class);
    }

    public <D> D getSession(Class<D> clss) throws RestException {
        RestConnection connection = new RestConnection.Builder()
            .properties(mProperties)
            .path(SESSION_URL)
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
			if(e.getStatusCode() == RestConnection.SC_PRECONDITION_FAILED) {
				return false;
			}
			throw e;
		}
    }

    public DefaultResponse deleteDatabase() throws RestException {
        return deleteDatabase(DefaultResponse.class);
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
    
    /* ADD API */

    public DefaultResponse add(Document document) throws RestException {
        return add(DefaultResponse.class, document);
    }

    public <R extends Response> R add(Class<R> clss, Document document) throws RestException {
        ensureDatabase();
        RestConnection connection = createConnection(null);
        R response = connection.post(clss, document);
        document.setId(response.getId());
        document.setRev(response.getRev());
        return response;
    }
    
    /* SAVE API */

    public DefaultResponse save(Document document) throws RestException {
        return save(DefaultResponse.class, document);
    }

    public <R extends Response> R save(Class<R> clss, Document document) throws RestException {
        ensureDatabase();
        ensureDocumentId(document.getId());
        RestConnection connection = createConnection(document.getId());
        R response = connection.put(clss, document);
		document.setRev(response.getRev());
		return response;
    }

    public DefaultResponse saveDesignDoc(String docId, Object object) throws RestException {
        return saveDesignDoc(DefaultResponse.class, docId, object);
    }

    public <R extends Response> R saveDesignDoc(Class<R> clss, String docId, Object object) throws RestException {
        ensureDatabase();
        ensureDocumentId(docId);
        RestConnection connection = createConnection(docId);
        return connection.put(clss, object);
    }
    
    /* DELETE API */

    public DefaultResponse delete(Document document) throws RestException {
        return delete(DefaultResponse.class, document);
    }

    public <R extends Response> R delete(Class<R> clss, Document document) throws RestException {
        ensureDatabase();
        ensureDocumentId(document.getId());
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(REVISION_PARAM, document.getRev());
        RestConnection connection = createConnection(document.getId(), params);
        R response = connection.delete(clss);
        document.setRev(response.getRev());
        return response;
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
            Reader reader = new InputStreamReader(new BufferedInputStream(connection.get(InputStream.class)), RestConnection.DEFAULT_CHARSET);
            return new ViewResult(reader);
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
            Reader reader = new InputStreamReader(new BufferedInputStream(connection.get(InputStream.class)), RestConnection.DEFAULT_CHARSET);
            return new StreamingViewResult(connection, reader);
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
            .path(path == null ? mDatabase : mDatabase + SEPARATOR + path, params)
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
}
