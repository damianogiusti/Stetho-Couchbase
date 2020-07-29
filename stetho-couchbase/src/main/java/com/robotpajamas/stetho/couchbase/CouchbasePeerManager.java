package com.robotpajamas.stetho.couchbase;

import android.content.Context;

import com.couchbase.lite.CouchbaseLiteException;
import com.couchbase.lite.DataSource;
import com.couchbase.lite.Meta;
import com.couchbase.lite.Ordering;
import com.couchbase.lite.QueryBuilder;
import com.couchbase.lite.Result;
import com.couchbase.lite.ResultSet;
import com.couchbase.lite.SelectResult;
import com.facebook.stetho.inspector.console.CLog;
import com.facebook.stetho.inspector.helper.ChromePeerManager;
import com.facebook.stetho.inspector.helper.PeerRegistrationListener;
import com.facebook.stetho.inspector.jsonrpc.JsonRpcPeer;
import com.facebook.stetho.inspector.protocol.module.Console;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import timber.log.Timber;

// TODO: Check support for ForestDB
// TODO: See if opening/closing of managers/database can be optimized
class CouchbasePeerManager extends ChromePeerManager {

    private static final String DOC_PATTERN = "\"(.*?)\"";
    private static final String DOC_ID_PATTERN = "^(<.+>::).+";
    private static final List<String> COLUMN_NAMES = Arrays.asList("key", "value");
    private static final String CBLITE_EXTENSION = ".cblite2";

    private final Pattern mPattern = Pattern.compile(DOC_PATTERN);
    private final Pattern mDocIdPattern = Pattern.compile(DOC_ID_PATTERN);

    private final String mPackageName;
    private final Context mContext;
    private final boolean mShowMetadata;

    CouchbasePeerManager(Context context, String packageName, boolean showMetadata) {
        mContext = context;
        mPackageName = packageName;
        mShowMetadata = showMetadata;

        setListener(new PeerRegistrationListener() {
            @Override
            public void onPeerRegistered(JsonRpcPeer peer) {
                setupPeer(peer);
            }

            @Override
            public void onPeerUnregistered(JsonRpcPeer peer) {

            }
        });
    }

    private void setupPeer(JsonRpcPeer peer) {
        List<String> potentialDatabases = getAllDatabaseNames();
        for (String database : potentialDatabases) {
            Timber.d("Datebase Name: %s", database);
            Database.DatabaseObject databaseParams = new Database.DatabaseObject();
            databaseParams.id = database;
            databaseParams.name = database;
            databaseParams.domain = mPackageName;
            databaseParams.version = "N/A";
            Database.AddDatabaseEvent eventParams = new Database.AddDatabaseEvent();
            eventParams.database = databaseParams;

            peer.invokeMethod("Database.addDatabase", eventParams, null /* callback */);
        }
    }

    private List<String> getAllDatabaseNames() {
        final File[] files = mContext.getFilesDir().listFiles();
        if (files != null) {
            List<String> databaseNames = new ArrayList<>(files.length);
            for (File file : files) {
                final String fileName = file.getName();
                if (fileName.endsWith(CBLITE_EXTENSION)) {
                    final String dbName = fileName.replace(CBLITE_EXTENSION, "");
                    databaseNames.add(dbName);
                }
            }
            return databaseNames;
        }
        return Collections.emptyList();
    }

    List<String> getAllDocumentIds(String databaseId) throws CouchbaseLiteException {
        Timber.d("getAllDocumentIds: %s", databaseId);

        com.couchbase.lite.Database database = null;
        try {
            database = new com.couchbase.lite.Database(databaseId);
            final ResultSet results = QueryBuilder.select(SelectResult.expression(Meta.id), SelectResult.property("type"))
                .from(DataSource.database(database))
                .orderBy(Ordering.expression(Meta.expiration).ascending())
                .execute();
            Set<String> docIds = new HashSet<>();
            for (Result result : results) {
                final String id = result.getString("id");
                final String type = result.getString("type");
                if (type == null) {
                    docIds.add(id);
                } else {
                    docIds.add(String.format("<%s>::%s", type, id));
                }
            }
            return new ArrayList<>(docIds);
        } catch (Exception e) {
            return Collections.emptyList();
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }

    Database.ExecuteSQLResponse executeSQL(String databaseId, String query) throws JSONException, IOException {
        Timber.d("executeSQL: %s, %s", databaseId, query);

        Database.ExecuteSQLResponse response = new Database.ExecuteSQLResponse();

        Matcher matcher = mPattern.matcher(query);
        if (!matcher.find()) {
            return response;
        }

        String docId = matcher.group(1);
        Timber.d("Parsed doc ID: %s", docId);

        Map<String, String> map = null;
        try {
            map = getDocument(databaseId, docId);
        } catch (CouchbaseLiteException e) {
            throw new IOException(e);
        }
        response.columnNames = COLUMN_NAMES;
        response.values = new ArrayList<>();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            final String key = entry.getKey();
            if (!mShowMetadata && key.substring(0,1).equals("_")) {
                continue;
            }
            response.values.add(key);
            response.values.add(entry.getValue());
        }

        // Log to console
        CLog.writeToConsole(Console.MessageLevel.DEBUG, Console.MessageSource.JAVASCRIPT, new JSONObject(map).toString(4));

        return response;
    }


    private Map<String, String> getDocument(String databaseId, String docId) throws CouchbaseLiteException {
        Timber.d("getDocument: %s, %s", databaseId, docId);
        com.couchbase.lite.Database database = null;
        try {
            database = new com.couchbase.lite.Database(databaseId);
            final Matcher matcher = mDocIdPattern.matcher(docId);
            if (matcher.matches()) {
                docId = docId.replaceFirst(matcher.group(1), "");
            }
            final Map<String, Object> doc = database.getDocument(docId).toMap();
            Map<String, String> returnedMap = new TreeMap<>();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                returnedMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return returnedMap;
        } catch (Exception e) {
            return Collections.emptyMap();
        } finally {
            if (database != null) {
                database.close();
            }
        }
    }
}
