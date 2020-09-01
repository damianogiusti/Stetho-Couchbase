package com.robotpajamas.android.stetho.couchbase;

import android.app.Application;
import android.content.Context;

import com.couchbase.lite.CouchbaseLite;
import com.couchbase.lite.Database;
import com.couchbase.lite.MutableDocument;
import com.facebook.stetho.Stetho;
import com.robotpajamas.stetho.couchbase.CouchbaseInspectorModulesProvider;

import java.util.HashMap;
import java.util.Map;

import timber.log.Timber;

public class MainApplication extends Application {

    private Database mDatabase;

    @Override
    public void onCreate() {
        super.onCreate();

        CouchbaseLite.init(this);

        if (BuildConfig.DEBUG) {
            Timber.plant(new Timber.DebugTree());
            Stetho.initialize(
                Stetho.newInitializerBuilder(this)
                    .enableDumpapp(Stetho.defaultDumperPluginsProvider(this))
                    .enableWebKitInspector(new CouchbaseInspectorModulesProvider.Builder(this)
                        .showMetadata(true) // Default: true
                        .build())
                    .build());
        }
        initializeCouchbase(this);
    }

    // Initialize database and create some fake data
    private void initializeCouchbase(Context context) {
        final Map<String, Object> s = new HashMap<>();
        s.put("type", "User");
        s.put("key1", "value1");
        s.put("key2", "value2");
        s.put("key3", "value3");
        s.put("key4", "value4");
        s.put("key5", "value5");
        s.put("key6", "value6");
        s.put("key7", "value7");
        s.put("key8", "value8");
        s.put("key9", "value9");
        s.put("key10", "value10");

        try {
            mDatabase = new Database("stetho-couchbase-sample");
            MutableDocument doc = new MutableDocument("id:123456");
            for (Map.Entry<String, Object> entry : s.entrySet()) {
                doc.setValue(entry.getKey(), entry.getValue());
            }
            mDatabase.save(doc);
            doc = new MutableDocument("id:123456");
            for (Map.Entry<String, Object> entry : s.entrySet()) {
                if (!"type".equals(entry.getKey())) {
                    doc.setValue(entry.getKey(), entry.getValue());
                }
            }
            mDatabase.save(doc);
            doc = new MutableDocument("id:123abc");
            for (Map.Entry<String, Object> entry : s.entrySet()) {
                doc.setValue(entry.getKey(), entry.getValue());
            }
            mDatabase.save(doc);
            mDatabase.close();
        } catch (Exception ignored) {
            ignored.printStackTrace();
        }
    }
}
