package com.netezza.util;

public interface NetezzaDBHelper {

	static final String DB_PROP_PREFIX = "db_";

    static final String DB_USER = "user";

    static final String PROP_DB_USER = DB_PROP_PREFIX + DB_USER;

    static final String DB_PASSWORD = "password";

    static final String PROP_DB_PASSWORD = DB_PROP_PREFIX + DB_PASSWORD;

    final static String DB_HOST = "host";

    static final String PROP_DB_HOST = DB_PROP_PREFIX + DB_HOST;

    final static String DB_PORT = "port";

    static final String PROP_DB_PORT = DB_PROP_PREFIX + DB_PORT;

    final static String DB_DB = "db";

    static final String PROP_DB_DB = DB_PROP_PREFIX + DB_DB;

    static final String DB_HELPER_CLASS_SUFFIX = "NetezzaDBHelper";

    static final String PROP_DB_CONNECT = "DBConnect";

    static final String PROP_SQL_STATEMENT = "SQLStmt";

    static final String DB_DATABASE = "database";

    static final String DB_CHARSET = "charset";

    static final String PROP_DB_CHARSET = DB_PROP_PREFIX + DB_CHARSET;

    static final String PROP_DB_DATABASE = DB_PROP_PREFIX + DB_DATABASE;
    
    

   
}
