// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Copied from Hplsql and modified for Apache Doris

lexer grammar PLLexer;

import DorisLexer;

// Lexer rules
ACTION: 'ACTION';
ALLOCATE: 'ALLOCATE';
ANSI_NULLS: 'ANSI_NULLS';
ANSI_PADDING: 'ANSI_PADDING';
ASSOCIATE: 'ASSOCIATE';
AVG: 'AVG';
BATCHSIZE: 'BATCHSIZE';
BINARY_DOUBLE: 'BINARY_DOUBLE';
BINARY_FLOAT: 'BINARY_FLOAT';
BINARY_INTEGER: 'BINARY_INTEGER';
BIT: 'BIT';
BODY: 'BODY';
BREAK: 'BREAK';
BULK: 'BULK';
BYTE: 'BYTE';
CALLER: 'CALLER';
CASCADE: 'CASCADE';
CASESPECIFIC: 'CASESPECIFIC';
CLIENT: 'CLIENT';
CLUSTERED: 'CLUSTERED';
CMP: 'CMP';
COLLECT: 'COLLECT';
COLLECTION: 'COLLECTION';
COMPRESS: 'COMPRESS';
CONCAT: 'CONCAT';
CONDITION: 'CONDITION';
CONTINUE: 'CONTINUE';
COUNT_BIG: 'COUNT_BIG';
CREATOR: 'CREATOR';
CS: 'CS';
CURRENT_SCHEMA: 'CURRENT_SCHEMA';
DAYS: 'DAYS';
DEC: 'DEC';
DEFINED: 'DEFINED';
DEFINER: 'DEFINER';
DEFINITION: 'DEFINITION';
DELIMITED: 'DELIMITED';
DELIMITER: 'DELIMITER';
DIAGNOSTICS: 'DIAGNOSTICS';
DIR: 'DIR';
DIRECTORY: 'DIRECTORY';
DISTRIBUTE: 'DISTRIBUTE';
ESCAPED: 'ESCAPED';
EXEC: 'EXEC';
EXCEPTION: 'EXCEPTION';
EXCLUSIVE: 'EXCLUSIVE';
EXIT: 'EXIT';
FALLBACK: 'FALLBACK';
FILES: 'FILES';
FOUND: 'FOUND';
GET: 'GET';
HANDLER: 'HANDLER';
HOST: 'HOST';
IDENTITY: 'IDENTITY';
INCLUDE: 'INCLUDE';
INITRANS: 'INITRANS';
INT2: 'INT2';
INT4: 'INT4';
INT8: 'INT8';
INVOKER: 'INVOKER';
ISOPEN: 'ISOPEN';
ITEMS: 'ITEMS';
KEEP: 'KEEP';
LANGUAGE: 'LANGUAGE';
LOCATOR: 'LOCATOR';
LOCATORS: 'LOCATORS';
LOCKS: 'LOCKS';
LOG: 'LOG';
LOGGED: 'LOGGED';
LOGGING: 'LOGGING';
MATCHED: 'MATCHED';
MAXTRANS: 'MAXTRANS';
MESSAGE_TEXT: 'MESSAGE_TEXT';
MICROSECOND: 'MICROSECOND';
MICROSECONDS: 'MICROSECONDS';
MULTISET: 'MULTISET';
NCHAR: 'NCHAR';
NEW: 'NEW';
NVARCHAR: 'NVARCHAR';
NOCOUNT: 'NOCOUNT';
NOCOMPRESS: 'NOCOMPRESS';
NOLOGGING: 'NOLOGGING';
NONE: 'NONE';
NOTFOUND: 'NOTFOUND';
NUMERIC: 'NUMERIC';
NUMBER: 'NUMBER';
OBJECT: 'OBJECT';
OFF: 'OFF';
OWNER: 'OWNER';
PACKAGE: 'PACKAGE';
PCTFREE: 'PCTFREE';
PCTUSED: 'PCTUSED';
PLS_INTEGER: 'PLS_INTEGER';
PRECISION: 'PRECISION';
PRESERVE: 'PRESERVE';
PRINT: 'PRINT';
QUALIFY: 'QUALIFY';
QUERY_BAND: 'QUERY_BAND';
QUIT: 'QUIT';
QUOTED_IDENTIFIER: 'QUOTED_IDENTIFIER';
RAISE: 'RAISE';
RESIGNAL: 'RESIGNAL';
RESTRICT: 'RESTRICT';
RESULT: 'RESULT';
RESULT_SET_LOCATOR: 'RESULT_SET_LOCATOR';
RETURN: 'RETURN';
REVERSE: 'REVERSE';
ROWTYPE: 'ROWTYPE';
ROW_COUNT: 'ROW_COUNT';
RR: 'RR';
RS: 'RS';
PWD: 'PWD';
SECONDS: 'SECONDS';
SECURITY: 'SECURITY';
SEGMENT: 'SEGMENT';
SEL: 'SEL';
SESSIONS: 'SESSIONS';
SHARE: 'SHARE';
SIGNAL: 'SIGNAL';
SIMPLE_DOUBLE: 'SIMPLE_DOUBLE';
SIMPLE_FLOAT: 'SIMPLE_FLOAT';
SIMPLE_INTEGER: 'SIMPLE_INTEGER';
SMALLDATETIME: 'SMALLDATETIME';
SQL: 'SQL';
SQLEXCEPTION: 'SQLEXCEPTION';
SQLINSERT: 'SQLINSERT';
SQLSTATE: 'SQLSTATE';
SQLWARNING: 'SQLWARNING';
STATISTICS: 'STATISTICS';
STEP: 'STEP';
STORED: 'STORED';
SUBDIR: 'SUBDIR';
SUBSTRING: 'SUBSTRING';
SUMMARY: 'SUMMARY';
SYS_REFCURSOR: 'SYS_REFCURSOR';
TABLESPACE: 'TABLESPACE';
TEXTIMAGE_ON: 'TEXTIMAGE_ON';
TITLE: 'TITLE';
TOP: 'TOP';
UR: 'UR';
VAR: 'VAR';
VARCHAR2: 'VARCHAR2';
VARYING: 'VARYING';
VOLATILE: 'VOLATILE';
WHILE: 'WHILE';
WITHOUT: 'WITHOUT';
XACT_ABORT: 'XACT_ABORT';
XML: 'XML';
YES: 'YES';

//Functionswithspecificsyntax
ACTIVITY_COUNT: 'ACTIVITY_COUNT';
CUME_DIST: 'CUME_DIST';
CURRENT_DATE: 'CURRENT_DATE';
CURRENT_TIMESTAMP: 'CURRENT_TIMESTAMP';
CURRENT_USER: 'CURRENT_USER';
DENSE_RANK: 'DENSE_RANK';
FIRST_VALUE: 'FIRST_VALUE';
LAG: 'LAG';
LAST_VALUE: 'LAST_VALUE';
LEAD: 'LEAD';
MAX_PART_STRING: 'MAX_PART_STRING';
MIN_PART_STRING: 'MIN_PART_STRING';
MAX_PART_INT: 'MAX_PART_INT';
MIN_PART_INT: 'MIN_PART_INT';
MAX_PART_DATE: 'MAX_PART_DATE';
MIN_PART_DATE: 'MIN_PART_DATE';
PART_COUNT: 'PART_COUNT';
PART_LOC: 'PART_LOC';
RANK: 'RANK';
ROW_NUMBER: 'ROW_NUMBER';
STDEV: 'STDEV';
SYSDATE: 'SYSDATE';
VARIANCE: 'VARIANCE';

DOT2: '..';

LABEL
    : ([a-zA-Z] | DIGIT | '_')* ':'
    ;

// Support case-insensitive keywords and allowing case-sensitive identifiers
//fragment A : ('a'|'A') ;
//fragment B : ('b'|'B') ;
//fragment C : ('c'|'C') ;
//fragment D : ('d'|'D') ;
//fragment E : ('e'|'E') ;
//fragment F : ('f'|'F') ;
//fragment G : ('g'|'G') ;
//fragment H : ('h'|'H') ;
//fragment I : ('i'|'I') ;
//fragment J : ('j'|'J') ;
//fragment K : ('k'|'K') ;
//fragment L : ('l'|'L') ;
//fragment M : ('m'|'M') ;
//fragment N : ('n'|'N') ;
//fragment O : ('o'|'O') ;
//fragment P : ('p'|'P') ;
//fragment Q : ('q'|'Q') ;
//fragment R : ('r'|'R') ;
//fragment S : ('s'|'S') ;
//fragment T : ('t'|'T') ;
//fragment U : ('u'|'U') ;
//fragment V : ('v'|'V') ;
//fragment W : ('w'|'W') ;
//fragment X : ('x'|'X') ;
//fragment Y : ('y'|'Y') ;
//fragment Z : ('z'|'Z') ;
