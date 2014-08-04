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


import java.util.HashMap;

import com.barenode.bareconnection.RestConnection;
import com.barenode.bareconnection.RestException;
import com.barenode.bareconnection.RestProperties;

public class CouchDbConnection {

    private final RestProperties mProperties;
    private final String mDatabase;


    public CouchDbConnection(RestProperties properties, String database) {
    	mProperties = properties;
    	mDatabase = database;
    }

    
    public boolean createDatabase() {
    	try {
    		RestConnection connection = createConnection("");
			connection.put();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 412 if database already exists
			if(e.getResponseCode() == 412) {
				return true;
			}
		}
    	return false;
    }

    public boolean contains(String docId) throws RestException {
		try {
    		RestConnection connection = createConnection("/" + docId);
			connection.head();
			return true;
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if(e.getResponseCode() == 404) {
				return false;
			}
			throw e;
		}
    }

    public <T extends Document>T get(Class<T> clss, String docId) throws RestException {
    	try {
    		RestConnection connection = createConnection("/" + docId);
			return connection.get(clss);
		} catch (RestException e) {
			// CouchDb returns a 404 Not Found if database doesn't contain document
			if(e.getResponseCode() == 404) {
				return null;
			}
			throw e;
		}
    }

    public Response save(Document document) throws RestException {
		RestConnection connection = createConnection("/" + document.getId());
		Response response = connection.put(document, Response.class);
		document.setRev(response.getRev());
		return response;
    }

    public Response delete(Document document) throws RestException {
		return delete(document.getId(), document.getRev());
    }

    public Response delete(String docId, String revId) throws RestException {
    	HashMap<String, String> params = new HashMap<String, String>();
    	params.put("rev", revId);
		RestConnection connection = new RestConnection.Builder()
			.properties(mProperties)
			.path(mDatabase + "/" + docId, params)
			.build();
		return connection.delete(Response.class);
    }

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
    	return new RestConnection.Builder()
			.properties(mProperties)
			.path(mDatabase + path)
			.build();
    }
}