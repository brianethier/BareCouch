package ca.barelabs.barecouch;


import java.io.IOException;
import java.util.Iterator;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


public class ViewIterator<T> implements Iterator<T> {

    private final Gson mGson = new Gson();
    private final JsonReader mJsonReader;
    private final Class<T> mClss;
    private final String mKey;


    public ViewIterator(JsonReader reader, Class<T> clss, String key) {
        mJsonReader = reader;
        mClss = clss;
        mKey = key;
    }

    
    @Override
    public boolean hasNext() {
        try {
            return mJsonReader.hasNext();
        }
        catch(IOException e) {
            throw new DatabaseAccessException(e);
        }
    }

	@SuppressWarnings("unchecked")
	@Override
	public T next() {
        try {
            T value = null;
            mJsonReader.beginObject();
            while(mJsonReader.hasNext()) {
                String nextName = mJsonReader.nextName();
                if (nextName.equals(mKey)) {
                    value = (T) mGson.fromJson(mJsonReader, mClss);
                } else {
                    mJsonReader.skipValue();
                }
            }
            mJsonReader.endObject();
            return value;
        }
        catch(IOException e) {
            throw new DatabaseAccessException(e);
        }
	}

	@Override
	public void remove() {
        throw new DatabaseAccessException("Not supported!");
	}
}
