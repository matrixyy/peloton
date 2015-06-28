/*-------------------------------------------------------------------------
 *
 * kwlist.h
 *
 * The keyword list is kept in its own source file for possible use by
 * automatic tools.  The exact representation of a keyword is determined
 * by the PG_KEYWORD macro, which is not defined in this file; it can
 * be defined by the caller for special purposes.
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/include/parser/kwlist.h
 *
 *-------------------------------------------------------------------------
 */

/* there is deliberately not an #ifndef KWLIST_H here */

/*
 * List of keyword (name, token-value, category) entries.
 *
 * !!WARNING!!: This list must be sorted by ASCII name, because binary
 *		 search is used to locate entries.
 */

/* name, value, category */
PG_KEYWORD("abort", ABORT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("absolute", ABSOLUTE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("access", ACCESS, UNRESERVED_KEYWORD)
PG_KEYWORD("action", ACTION, UNRESERVED_KEYWORD)
PG_KEYWORD("add", ADD_P, UNRESERVED_KEYWORD)
PG_KEYWORD("admin", ADMIN, UNRESERVED_KEYWORD)
PG_KEYWORD("after", AFTER, UNRESERVED_KEYWORD)
PG_KEYWORD("aggregate", AGGREGATE, UNRESERVED_KEYWORD)
PG_KEYWORD("all", ALL, RESERVED_KEYWORD)
PG_KEYWORD("also", ALSO, UNRESERVED_KEYWORD)
PG_KEYWORD("alter", ALTER, UNRESERVED_KEYWORD)
PG_KEYWORD("always", ALWAYS, UNRESERVED_KEYWORD)
PG_KEYWORD("analyse", ANALYSE, RESERVED_KEYWORD)		/* British spelling */
PG_KEYWORD("analyze", ANALYZE, RESERVED_KEYWORD)
PG_KEYWORD("and", AND, RESERVED_KEYWORD)
PG_KEYWORD("any", ANY, RESERVED_KEYWORD)
PG_KEYWORD("array", ARRAY, RESERVED_KEYWORD)
PG_KEYWORD("as", AS, RESERVED_KEYWORD)
PG_KEYWORD("asc", ASC, RESERVED_KEYWORD)
PG_KEYWORD("assertion", ASSERTION, UNRESERVED_KEYWORD)
PG_KEYWORD("assignment", ASSIGNMENT, UNRESERVED_KEYWORD)
PG_KEYWORD("asymmetric", ASYMMETRIC, RESERVED_KEYWORD)
PG_KEYWORD("at", AT, UNRESERVED_KEYWORD)
PG_KEYWORD("attribute", ATTRIBUTE, UNRESERVED_KEYWORD)
PG_KEYWORD("authorization", AUTHORIZATION, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("backward", BACKWARD, UNRESERVED_KEYWORD)
PG_KEYWORD("before", BEFORE, UNRESERVED_KEYWORD)
PG_KEYWORD("begin", BEGIN_P, UNRESERVED_KEYWORD)
PG_KEYWORD("between", BETWEEN, COL_NAME_KEYWORD)
PG_KEYWORD("bigint", BIGINT, COL_NAME_KEYWORD)
PG_KEYWORD("binary", BINARY, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("bit", BIT, COL_NAME_KEYWORD)
PG_KEYWORD("boolean", BOOLEAN_P, COL_NAME_KEYWORD)
PG_KEYWORD("both", BOTH, RESERVED_KEYWORD)
PG_KEYWORD("by", BY, UNRESERVED_KEYWORD)
PG_KEYWORD("cache", CACHE, UNRESERVED_KEYWORD)
PG_KEYWORD("called", CALLED, UNRESERVED_KEYWORD)
PG_KEYWORD("cascade", CASCADE, UNRESERVED_KEYWORD)
PG_KEYWORD("cascaded", CASCADED, UNRESERVED_KEYWORD)
PG_KEYWORD("case", CASE, RESERVED_KEYWORD)
PG_KEYWORD("cast", CAST, RESERVED_KEYWORD)
PG_KEYWORD("catalog", CATALOG_P, UNRESERVED_KEYWORD)
PG_KEYWORD("chain", CHAIN, UNRESERVED_KEYWORD)
PG_KEYWORD("char", CHAR_P, COL_NAME_KEYWORD)
PG_KEYWORD("character", CHARACTER, COL_NAME_KEYWORD)
PG_KEYWORD("characteristics", CHARACTERISTICS, UNRESERVED_KEYWORD)
PG_KEYWORD("check", CHECK, RESERVED_KEYWORD)
PG_KEYWORD("checkpoint", CHECKPOINT, UNRESERVED_KEYWORD)
PG_KEYWORD("cclass", CLASS, UNRESERVED_KEYWORD)
PG_KEYWORD("close", CLOSE, UNRESERVED_KEYWORD)
PG_KEYWORD("cluster", CLUSTER, UNRESERVED_KEYWORD)
PG_KEYWORD("coalesce", COALESCE, COL_NAME_KEYWORD)
PG_KEYWORD("collate", COLLATE, RESERVED_KEYWORD)
PG_KEYWORD("collation", COLLATION, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("column", COLUMN, RESERVED_KEYWORD)
PG_KEYWORD("comment", COMMENT, UNRESERVED_KEYWORD)
PG_KEYWORD("comments", COMMENTS, UNRESERVED_KEYWORD)
PG_KEYWORD("commit", COMMIT, UNRESERVED_KEYWORD)
PG_KEYWORD("committed", COMMITTED, UNRESERVED_KEYWORD)
PG_KEYWORD("concurrently", CONCURRENTLY, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("configuration", CONFIGURATION, UNRESERVED_KEYWORD)
PG_KEYWORD("conflict", CONFLICT, UNRESERVED_KEYWORD)
PG_KEYWORD("connection", CONNECTION, UNRESERVED_KEYWORD)
PG_KEYWORD("constraint", CONSTRAINT, RESERVED_KEYWORD)
PG_KEYWORD("constraints", CONSTRAINTS, UNRESERVED_KEYWORD)
PG_KEYWORD("content", CONTENT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("continue", CONTINUE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("conversion", CONVERSION_P, UNRESERVED_KEYWORD)
PG_KEYWORD("copy", COPY, UNRESERVED_KEYWORD)
PG_KEYWORD("cost", COST, UNRESERVED_KEYWORD)
PG_KEYWORD("create", CREATE, RESERVED_KEYWORD)
PG_KEYWORD("cross", CROSS, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("csv", CSV, UNRESERVED_KEYWORD)
PG_KEYWORD("cube", CUBE, UNRESERVED_KEYWORD)
PG_KEYWORD("current", CURRENT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("current_catalog", CURRENT_CATALOG, RESERVED_KEYWORD)
PG_KEYWORD("current_date", CURRENT_DATE, RESERVED_KEYWORD)
PG_KEYWORD("current_role", CURRENT_ROLE, RESERVED_KEYWORD)
PG_KEYWORD("current_schema", CURRENT_SCHEMA, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("current_time", CURRENT_TIME, RESERVED_KEYWORD)
PG_KEYWORD("current_timestamp", CURRENT_TIMESTAMP, RESERVED_KEYWORD)
PG_KEYWORD("current_user", CURRENT_USER, RESERVED_KEYWORD)
PG_KEYWORD("cursor", CURSOR, UNRESERVED_KEYWORD)
PG_KEYWORD("cycle", CYCLE, UNRESERVED_KEYWORD)
PG_KEYWORD("data", DATA_P, UNRESERVED_KEYWORD)
PG_KEYWORD("database", DATABASE, UNRESERVED_KEYWORD)
PG_KEYWORD("day", DAY_P, UNRESERVED_KEYWORD)
PG_KEYWORD("deallocate", DEALLOCATE, UNRESERVED_KEYWORD)
PG_KEYWORD("dec", DEC, COL_NAME_KEYWORD)
PG_KEYWORD("decimal", DECIMAL_P, COL_NAME_KEYWORD)
PG_KEYWORD("declare", DECLARE, UNRESERVED_KEYWORD)
PG_KEYWORD("default", DEFAULT, RESERVED_KEYWORD)
PG_KEYWORD("defaults", DEFAULTS, UNRESERVED_KEYWORD)
PG_KEYWORD("deferrable", DEFERRABLE, RESERVED_KEYWORD)
PG_KEYWORD("deferred", DEFERRED, UNRESERVED_KEYWORD)
PG_KEYWORD("definer", DEFINER, UNRESERVED_KEYWORD)
PG_KEYWORD("delete", DELETE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("delimiter", DELIMITER, UNRESERVED_KEYWORD)
PG_KEYWORD("delimiters", DELIMITERS, UNRESERVED_KEYWORD)
PG_KEYWORD("desc", DESC, RESERVED_KEYWORD)
PG_KEYWORD("dictionary", DICTIONARY, UNRESERVED_KEYWORD)
PG_KEYWORD("disable", DISABLE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("discard", DISCARD, UNRESERVED_KEYWORD)
PG_KEYWORD("distinct", DISTINCT, RESERVED_KEYWORD)
PG_KEYWORD("do", DO, RESERVED_KEYWORD)
PG_KEYWORD("document", DOCUMENT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("domain", DOMAIN_P, UNRESERVED_KEYWORD)
PG_KEYWORD("double", DOUBLE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("drop", DROP, UNRESERVED_KEYWORD)
PG_KEYWORD("each", EACH, UNRESERVED_KEYWORD)
PG_KEYWORD("else", ELSE, RESERVED_KEYWORD)
PG_KEYWORD("enable", ENABLE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("encoding", ENCODING, UNRESERVED_KEYWORD)
PG_KEYWORD("encrypted", ENCRYPTED, UNRESERVED_KEYWORD)
PG_KEYWORD("end", END_P, RESERVED_KEYWORD)
PG_KEYWORD("enum", ENUM_P, UNRESERVED_KEYWORD)
PG_KEYWORD("escape", ESCAPE, UNRESERVED_KEYWORD)
PG_KEYWORD("event", EVENT, UNRESERVED_KEYWORD)
PG_KEYWORD("except", EXCEPT, RESERVED_KEYWORD)
PG_KEYWORD("exclude", EXCLUDE, UNRESERVED_KEYWORD)
PG_KEYWORD("excluding", EXCLUDING, UNRESERVED_KEYWORD)
PG_KEYWORD("exclusive", EXCLUSIVE, UNRESERVED_KEYWORD)
PG_KEYWORD("execute", EXECUTE, UNRESERVED_KEYWORD)
PG_KEYWORD("exists", EXISTS, COL_NAME_KEYWORD)
PG_KEYWORD("explain", EXPLAIN, UNRESERVED_KEYWORD)
PG_KEYWORD("extension", EXTENSION, UNRESERVED_KEYWORD)
PG_KEYWORD("external", EXTERNAL, UNRESERVED_KEYWORD)
PG_KEYWORD("extract", EXTRACT, COL_NAME_KEYWORD)
PG_KEYWORD("false", FALSE_P, RESERVED_KEYWORD)
PG_KEYWORD("family", FAMILY, UNRESERVED_KEYWORD)
PG_KEYWORD("fetch", FETCH, RESERVED_KEYWORD)
PG_KEYWORD("filter", FILTER, UNRESERVED_KEYWORD)
PG_KEYWORD("first", FIRST_P, UNRESERVED_KEYWORD)
PG_KEYWORD("float", FLOAT_P, COL_NAME_KEYWORD)
PG_KEYWORD("following", FOLLOWING, UNRESERVED_KEYWORD)
PG_KEYWORD("for", FOR, RESERVED_KEYWORD)
PG_KEYWORD("force", FORCE, UNRESERVED_KEYWORD)
PG_KEYWORD("foreign", FOREIGN, RESERVED_KEYWORD)
PG_KEYWORD("forward", FORWARD, UNRESERVED_KEYWORD)
PG_KEYWORD("freeze", FREEZE, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("from", FROM, RESERVED_KEYWORD)
PG_KEYWORD("full", FULL, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("function", FUNCTION, UNRESERVED_KEYWORD)
PG_KEYWORD("functions", FUNCTIONS, UNRESERVED_KEYWORD)
PG_KEYWORD("global", GLOBAL, UNRESERVED_KEYWORD)
PG_KEYWORD("grant", GRANT, RESERVED_KEYWORD)
PG_KEYWORD("granted", GRANTED, UNRESERVED_KEYWORD)
PG_KEYWORD("greatest", GREATEST, COL_NAME_KEYWORD)
PG_KEYWORD("group", GROUP_P, RESERVED_KEYWORD)
PG_KEYWORD("grouping", GROUPING, COL_NAME_KEYWORD)
PG_KEYWORD("handler", HANDLER, UNRESERVED_KEYWORD)
PG_KEYWORD("having", HAVING, RESERVED_KEYWORD)
PG_KEYWORD("header", HEADER_P, UNRESERVED_KEYWORD)
PG_KEYWORD("hold", HOLD, UNRESERVED_KEYWORD)
PG_KEYWORD("hour", HOUR_P, UNRESERVED_KEYWORD)
PG_KEYWORD("identity", IDENTITY_P, UNRESERVED_KEYWORD)
PG_KEYWORD("if", IF_P, UNRESERVED_KEYWORD)
PG_KEYWORD("ilike", ILIKE, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("immediate", IMMEDIATE, UNRESERVED_KEYWORD)
PG_KEYWORD("immutable", IMMUTABLE, UNRESERVED_KEYWORD)
PG_KEYWORD("implicit", IMPLICIT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("import", IMPORT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("in", IN_P, RESERVED_KEYWORD)
PG_KEYWORD("including", INCLUDING, UNRESERVED_KEYWORD)
PG_KEYWORD("increment", INCREMENT, UNRESERVED_KEYWORD)
PG_KEYWORD("index", INDEX, UNRESERVED_KEYWORD)
PG_KEYWORD("indexes", INDEXES, UNRESERVED_KEYWORD)
PG_KEYWORD("inherit", INHERIT, UNRESERVED_KEYWORD)
PG_KEYWORD("inherits", INHERITS, UNRESERVED_KEYWORD)
PG_KEYWORD("initially", INITIALLY, RESERVED_KEYWORD)
PG_KEYWORD("inline", INLINE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("inner", INNER_P, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("inout", INOUT, COL_NAME_KEYWORD)
PG_KEYWORD("input", INPUT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("insensitive", INSENSITIVE, UNRESERVED_KEYWORD)
PG_KEYWORD("insert", INSERT, UNRESERVED_KEYWORD)
PG_KEYWORD("instead", INSTEAD, UNRESERVED_KEYWORD)
PG_KEYWORD("int", INT_P, COL_NAME_KEYWORD)
PG_KEYWORD("integer", INTEGER, COL_NAME_KEYWORD)
PG_KEYWORD("intersect", INTERSECT, RESERVED_KEYWORD)
PG_KEYWORD("interval", INTERVAL, COL_NAME_KEYWORD)
PG_KEYWORD("into", INTO, RESERVED_KEYWORD)
PG_KEYWORD("invoker", INVOKER, UNRESERVED_KEYWORD)
PG_KEYWORD("is", IS, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("isnull", ISNULL, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("isolation", ISOLATION, UNRESERVED_KEYWORD)
PG_KEYWORD("join", JOIN, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("key", KEY, UNRESERVED_KEYWORD)
PG_KEYWORD("label", LABEL, UNRESERVED_KEYWORD)
PG_KEYWORD("language", LANGUAGE, UNRESERVED_KEYWORD)
PG_KEYWORD("large", LARGE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("last", LAST_P, UNRESERVED_KEYWORD)
PG_KEYWORD("lateral", LATERAL_P, RESERVED_KEYWORD)
PG_KEYWORD("leading", LEADING, RESERVED_KEYWORD)
PG_KEYWORD("leakproof", LEAKPROOF, UNRESERVED_KEYWORD)
PG_KEYWORD("least", LEAST, COL_NAME_KEYWORD)
PG_KEYWORD("left", LEFT, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("level", LEVEL, UNRESERVED_KEYWORD)
PG_KEYWORD("like", LIKE, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("limit", LIMIT, RESERVED_KEYWORD)
PG_KEYWORD("listen", LISTEN, UNRESERVED_KEYWORD)
PG_KEYWORD("load", LOAD, UNRESERVED_KEYWORD)
PG_KEYWORD("local", LOCAL, UNRESERVED_KEYWORD)
PG_KEYWORD("localtime", LOCALTIME, RESERVED_KEYWORD)
PG_KEYWORD("localtimestamp", LOCALTIMESTAMP, RESERVED_KEYWORD)
PG_KEYWORD("location", LOCATION, UNRESERVED_KEYWORD)
PG_KEYWORD("lock", LOCK_P, UNRESERVED_KEYWORD)
PG_KEYWORD("locked", LOCKED, UNRESERVED_KEYWORD)
PG_KEYWORD("logged", LOGGED, UNRESERVED_KEYWORD)
PG_KEYWORD("mapping", MAPPING, UNRESERVED_KEYWORD)
PG_KEYWORD("match", MATCH, UNRESERVED_KEYWORD)
PG_KEYWORD("materialized", MATERIALIZED, UNRESERVED_KEYWORD)
PG_KEYWORD("maxvalue", MAXVALUE, UNRESERVED_KEYWORD)
PG_KEYWORD("minute", MINUTE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("minvalue", MINVALUE, UNRESERVED_KEYWORD)
PG_KEYWORD("mode", MODE, UNRESERVED_KEYWORD)
PG_KEYWORD("month", MONTH_P, UNRESERVED_KEYWORD)
PG_KEYWORD("move", MOVE, UNRESERVED_KEYWORD)
PG_KEYWORD("name", NAME_P, UNRESERVED_KEYWORD)
PG_KEYWORD("names", NAMES, UNRESERVED_KEYWORD)
PG_KEYWORD("national", NATIONAL, COL_NAME_KEYWORD)
PG_KEYWORD("natural", NATURAL, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("nchar", NCHAR, COL_NAME_KEYWORD)
PG_KEYWORD("next", NEXT, UNRESERVED_KEYWORD)
PG_KEYWORD("no", NO, UNRESERVED_KEYWORD)
PG_KEYWORD("none", NONE, COL_NAME_KEYWORD)
PG_KEYWORD("not", NOT, RESERVED_KEYWORD)
PG_KEYWORD("nothing", NOTHING, UNRESERVED_KEYWORD)
PG_KEYWORD("notify", NOTIFY, UNRESERVED_KEYWORD)
PG_KEYWORD("notnull", NOTNULL, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("nowait", NOWAIT, UNRESERVED_KEYWORD)
PG_KEYWORD("null", NULL_P, RESERVED_KEYWORD)
PG_KEYWORD("nullif", NULLIF, COL_NAME_KEYWORD)
PG_KEYWORD("nulls", NULLS_P, UNRESERVED_KEYWORD)
PG_KEYWORD("numeric", NUMERIC, COL_NAME_KEYWORD)
PG_KEYWORD("object", OBJECT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("of", OF, UNRESERVED_KEYWORD)
PG_KEYWORD("off", OFF, UNRESERVED_KEYWORD)
PG_KEYWORD("offset", OFFSET, RESERVED_KEYWORD)
PG_KEYWORD("oids", OIDS, UNRESERVED_KEYWORD)
PG_KEYWORD("on", ON, RESERVED_KEYWORD)
PG_KEYWORD("only", ONLY, RESERVED_KEYWORD)
PG_KEYWORD("coperator", OPERATOR, UNRESERVED_KEYWORD)
PG_KEYWORD("option", OPTION, UNRESERVED_KEYWORD)
PG_KEYWORD("options", OPTIONS, UNRESERVED_KEYWORD)
PG_KEYWORD("or", OR, RESERVED_KEYWORD)
PG_KEYWORD("order", ORDER, RESERVED_KEYWORD)
PG_KEYWORD("ordinality", ORDINALITY, UNRESERVED_KEYWORD)
PG_KEYWORD("out", OUT_P, COL_NAME_KEYWORD)
PG_KEYWORD("outer", OUTER_P, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("over", OVER, UNRESERVED_KEYWORD)
PG_KEYWORD("overlaps", OVERLAPS, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("overlay", OVERLAY, COL_NAME_KEYWORD)
PG_KEYWORD("owned", OWNED, UNRESERVED_KEYWORD)
PG_KEYWORD("owner", OWNER, UNRESERVED_KEYWORD)
PG_KEYWORD("parser", PARSER, UNRESERVED_KEYWORD)
PG_KEYWORD("partial", PARTIAL, UNRESERVED_KEYWORD)
PG_KEYWORD("partition", PARTITION, UNRESERVED_KEYWORD)
PG_KEYWORD("passing", PASSING, UNRESERVED_KEYWORD)
PG_KEYWORD("password", PASSWORD, UNRESERVED_KEYWORD)
PG_KEYWORD("placing", PLACING, RESERVED_KEYWORD)
PG_KEYWORD("plans", PLANS, UNRESERVED_KEYWORD)
PG_KEYWORD("policy", POLICY, UNRESERVED_KEYWORD)
PG_KEYWORD("position", POSITION, COL_NAME_KEYWORD)
PG_KEYWORD("preceding", PRECEDING, UNRESERVED_KEYWORD)
PG_KEYWORD("precision", PRECISION, COL_NAME_KEYWORD)
PG_KEYWORD("prepare", PREPARE, UNRESERVED_KEYWORD)
PG_KEYWORD("prepared", PREPARED, UNRESERVED_KEYWORD)
PG_KEYWORD("preserve", PRESERVE, UNRESERVED_KEYWORD)
PG_KEYWORD("primary", PRIMARY, RESERVED_KEYWORD)
PG_KEYWORD("prior", PRIOR, UNRESERVED_KEYWORD)
PG_KEYWORD("privileges", PRIVILEGES, UNRESERVED_KEYWORD)
PG_KEYWORD("procedural", PROCEDURAL, UNRESERVED_KEYWORD)
PG_KEYWORD("procedure", PROCEDURE, UNRESERVED_KEYWORD)
PG_KEYWORD("program", PROGRAM, UNRESERVED_KEYWORD)
PG_KEYWORD("quote", QUOTE, UNRESERVED_KEYWORD)
PG_KEYWORD("range", RANGE, UNRESERVED_KEYWORD)
PG_KEYWORD("read", READ, UNRESERVED_KEYWORD)
PG_KEYWORD("real", REAL, COL_NAME_KEYWORD)
PG_KEYWORD("reassign", REASSIGN, UNRESERVED_KEYWORD)
PG_KEYWORD("recheck", RECHECK, UNRESERVED_KEYWORD)
PG_KEYWORD("recursive", RECURSIVE, UNRESERVED_KEYWORD)
PG_KEYWORD("ref", REF, UNRESERVED_KEYWORD)
PG_KEYWORD("references", REFERENCES, RESERVED_KEYWORD)
PG_KEYWORD("refresh", REFRESH, UNRESERVED_KEYWORD)
PG_KEYWORD("reindex", REINDEX, UNRESERVED_KEYWORD)
PG_KEYWORD("relative", RELATIVE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("release", RELEASE, UNRESERVED_KEYWORD)
PG_KEYWORD("rename", RENAME, UNRESERVED_KEYWORD)
PG_KEYWORD("repeatable", REPEATABLE, UNRESERVED_KEYWORD)
PG_KEYWORD("replace", REPLACE, UNRESERVED_KEYWORD)
PG_KEYWORD("replica", REPLICA, UNRESERVED_KEYWORD)
PG_KEYWORD("reset", RESET, UNRESERVED_KEYWORD)
PG_KEYWORD("restart", RESTART, UNRESERVED_KEYWORD)
PG_KEYWORD("restrict", RESTRICT, UNRESERVED_KEYWORD)
PG_KEYWORD("returning", RETURNING, RESERVED_KEYWORD)
PG_KEYWORD("returns", RETURNS, UNRESERVED_KEYWORD)
PG_KEYWORD("revoke", REVOKE, UNRESERVED_KEYWORD)
PG_KEYWORD("right", RIGHT, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("role", ROLE, UNRESERVED_KEYWORD)
PG_KEYWORD("rollback", ROLLBACK, UNRESERVED_KEYWORD)
PG_KEYWORD("rollup", ROLLUP, UNRESERVED_KEYWORD)
PG_KEYWORD("row", ROW, COL_NAME_KEYWORD)
PG_KEYWORD("rows", ROWS, UNRESERVED_KEYWORD)
PG_KEYWORD("rule", RULE, UNRESERVED_KEYWORD)
PG_KEYWORD("savepoint", SAVEPOINT, UNRESERVED_KEYWORD)
PG_KEYWORD("schema", SCHEMA, UNRESERVED_KEYWORD)
PG_KEYWORD("scroll", SCROLL, UNRESERVED_KEYWORD)
PG_KEYWORD("search", SEARCH, UNRESERVED_KEYWORD)
PG_KEYWORD("second", SECOND_P, UNRESERVED_KEYWORD)
PG_KEYWORD("security", SECURITY, UNRESERVED_KEYWORD)
PG_KEYWORD("select", SELECT, RESERVED_KEYWORD)
PG_KEYWORD("sequence", SEQUENCE, UNRESERVED_KEYWORD)
PG_KEYWORD("sequences", SEQUENCES, UNRESERVED_KEYWORD)
PG_KEYWORD("serializable", SERIALIZABLE, UNRESERVED_KEYWORD)
PG_KEYWORD("server", SERVER, UNRESERVED_KEYWORD)
PG_KEYWORD("session", SESSION, UNRESERVED_KEYWORD)
PG_KEYWORD("session_user", SESSION_USER, RESERVED_KEYWORD)
PG_KEYWORD("set", SET, UNRESERVED_KEYWORD)
PG_KEYWORD("setof", SETOF, COL_NAME_KEYWORD)
PG_KEYWORD("sets", SETS, UNRESERVED_KEYWORD)
PG_KEYWORD("share", SHARE, UNRESERVED_KEYWORD)
PG_KEYWORD("show", SHOW, UNRESERVED_KEYWORD)
PG_KEYWORD("similar", SIMILAR, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("simple", SIMPLE, UNRESERVED_KEYWORD)
PG_KEYWORD("skip", SKIP, UNRESERVED_KEYWORD)
PG_KEYWORD("smallint", SMALLINT, COL_NAME_KEYWORD)
PG_KEYWORD("snapshot", SNAPSHOT, UNRESERVED_KEYWORD)
PG_KEYWORD("some", SOME, RESERVED_KEYWORD)
PG_KEYWORD("sql", SQL_P, UNRESERVED_KEYWORD)
PG_KEYWORD("stable", STABLE, UNRESERVED_KEYWORD)
PG_KEYWORD("standalone", STANDALONE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("start", START, UNRESERVED_KEYWORD)
PG_KEYWORD("statement", STATEMENT, UNRESERVED_KEYWORD)
PG_KEYWORD("statistics", STATISTICS, UNRESERVED_KEYWORD)
PG_KEYWORD("stdin", STDIN, UNRESERVED_KEYWORD)
PG_KEYWORD("stdout", STDOUT, UNRESERVED_KEYWORD)
PG_KEYWORD("storage", STORAGE, UNRESERVED_KEYWORD)
PG_KEYWORD("strict", STRICT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("strip", STRIP_P, UNRESERVED_KEYWORD)
PG_KEYWORD("substring", SUBSTRING, COL_NAME_KEYWORD)
PG_KEYWORD("symmetric", SYMMETRIC, RESERVED_KEYWORD)
PG_KEYWORD("sysid", SYSID, UNRESERVED_KEYWORD)
PG_KEYWORD("system", SYSTEM_P, UNRESERVED_KEYWORD)
PG_KEYWORD("table", TABLE, RESERVED_KEYWORD)
PG_KEYWORD("tables", TABLES, UNRESERVED_KEYWORD)
PG_KEYWORD("tablesample", TABLESAMPLE, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("tablespace", TABLESPACE, UNRESERVED_KEYWORD)
PG_KEYWORD("temp", TEMP, UNRESERVED_KEYWORD)
PG_KEYWORD("ctemplate", TEMPLATE, UNRESERVED_KEYWORD)
PG_KEYWORD("temporary", TEMPORARY, UNRESERVED_KEYWORD)
PG_KEYWORD("text", TEXT_P, UNRESERVED_KEYWORD)
PG_KEYWORD("then", THEN, RESERVED_KEYWORD)
PG_KEYWORD("time", TIME, COL_NAME_KEYWORD)
PG_KEYWORD("timestamp", TIMESTAMP, COL_NAME_KEYWORD)
PG_KEYWORD("to", TO, RESERVED_KEYWORD)
PG_KEYWORD("trailing", TRAILING, RESERVED_KEYWORD)
PG_KEYWORD("transaction", TRANSACTION, UNRESERVED_KEYWORD)
PG_KEYWORD("transform", TRANSFORM, UNRESERVED_KEYWORD)
PG_KEYWORD("treat", TREAT, COL_NAME_KEYWORD)
PG_KEYWORD("trigger", TRIGGER, UNRESERVED_KEYWORD)
PG_KEYWORD("trim", TRIM, COL_NAME_KEYWORD)
PG_KEYWORD("true", TRUE_P, RESERVED_KEYWORD)
PG_KEYWORD("truncate", TRUNCATE, UNRESERVED_KEYWORD)
PG_KEYWORD("trusted", TRUSTED, UNRESERVED_KEYWORD)
PG_KEYWORD("type", TYPE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("types", TYPES_P, UNRESERVED_KEYWORD)
PG_KEYWORD("unbounded", UNBOUNDED, UNRESERVED_KEYWORD)
PG_KEYWORD("uncommitted", UNCOMMITTED, UNRESERVED_KEYWORD)
PG_KEYWORD("unencrypted", UNENCRYPTED, UNRESERVED_KEYWORD)
PG_KEYWORD("union", UNION, RESERVED_KEYWORD)
PG_KEYWORD("unique", UNIQUE, RESERVED_KEYWORD)
PG_KEYWORD("unknown", UNKNOWN, UNRESERVED_KEYWORD)
PG_KEYWORD("unlisten", UNLISTEN, UNRESERVED_KEYWORD)
PG_KEYWORD("unlogged", UNLOGGED, UNRESERVED_KEYWORD)
PG_KEYWORD("until", UNTIL, UNRESERVED_KEYWORD)
PG_KEYWORD("update", UPDATE, UNRESERVED_KEYWORD)
PG_KEYWORD("user", USER, RESERVED_KEYWORD)
PG_KEYWORD("using", USING, RESERVED_KEYWORD)
PG_KEYWORD("vacuum", VACUUM, UNRESERVED_KEYWORD)
PG_KEYWORD("valid", VALID, UNRESERVED_KEYWORD)
PG_KEYWORD("validate", VALIDATE, UNRESERVED_KEYWORD)
PG_KEYWORD("validator", VALIDATOR, UNRESERVED_KEYWORD)
PG_KEYWORD("value", VALUE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("values", VALUES, COL_NAME_KEYWORD)
PG_KEYWORD("varchar", VARCHAR, COL_NAME_KEYWORD)
PG_KEYWORD("variadic", VARIADIC, RESERVED_KEYWORD)
PG_KEYWORD("varying", VARYING, UNRESERVED_KEYWORD)
PG_KEYWORD("verbose", VERBOSE, TYPE_FUNC_NAME_KEYWORD)
PG_KEYWORD("version", VERSION_P, UNRESERVED_KEYWORD)
PG_KEYWORD("view", VIEW, UNRESERVED_KEYWORD)
PG_KEYWORD("views", VIEWS, UNRESERVED_KEYWORD)
PG_KEYWORD("volatile", VOLATILE, UNRESERVED_KEYWORD)
PG_KEYWORD("when", WHEN, RESERVED_KEYWORD)
PG_KEYWORD("where", WHERE, RESERVED_KEYWORD)
PG_KEYWORD("whitespace", WHITESPACE_P, UNRESERVED_KEYWORD)
PG_KEYWORD("window", WINDOW, RESERVED_KEYWORD)
PG_KEYWORD("with", WITH, RESERVED_KEYWORD)
PG_KEYWORD("within", WITHIN, UNRESERVED_KEYWORD)
PG_KEYWORD("without", WITHOUT, UNRESERVED_KEYWORD)
PG_KEYWORD("work", WORK, UNRESERVED_KEYWORD)
PG_KEYWORD("wrapper", WRAPPER, UNRESERVED_KEYWORD)
PG_KEYWORD("write", WRITE, UNRESERVED_KEYWORD)
PG_KEYWORD("xml", XML_P, UNRESERVED_KEYWORD)
PG_KEYWORD("xmlattributes", XMLATTRIBUTES, COL_NAME_KEYWORD)
PG_KEYWORD("xmlconcat", XMLCONCAT, COL_NAME_KEYWORD)
PG_KEYWORD("xmlelement", XMLELEMENT, COL_NAME_KEYWORD)
PG_KEYWORD("xmlexists", XMLEXISTS, COL_NAME_KEYWORD)
PG_KEYWORD("xmlforest", XMLFOREST, COL_NAME_KEYWORD)
PG_KEYWORD("xmlparse", XMLPARSE, COL_NAME_KEYWORD)
PG_KEYWORD("xmlpi", XMLPI, COL_NAME_KEYWORD)
PG_KEYWORD("xmlroot", XMLROOT, COL_NAME_KEYWORD)
PG_KEYWORD("xmlserialize", XMLSERIALIZE, COL_NAME_KEYWORD)
PG_KEYWORD("year", YEAR_P, UNRESERVED_KEYWORD)
PG_KEYWORD("yes", YES_P, UNRESERVED_KEYWORD)
PG_KEYWORD("zone", ZONE, UNRESERVED_KEYWORD)
