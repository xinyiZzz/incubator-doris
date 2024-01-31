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
// This file is copied from
// https://github.com/apache/hive/blob/master/hplsql/src/main/antlr4/org/apache/hive/hplsql/Plsql.g4
// and modified by Doris

// PL/SQL Procedural SQL Extension Grammar
grammar PLParser;

options { tokenVocab = DorisLexer; }

import DorisParser;

program : block EOF;

block : ((begin_end_block | stmt) T_GO?)+ ;               // Multiple consecutive blocks/statements

begin_end_block :
       declare_block? T_BEGIN block exception_block? block_end
     ;

single_block_stmt :                                      // Single BEGIN END block (but nested blocks are possible) or single statement
       T_BEGIN block exception_block? block_end
     | stmt T_SEMICOLON?
     ;

block_end :
       {!_input.LT(2).getText().equalsIgnoreCase("TRANSACTION")}? T_END
     ;

procedure_block :
       begin_end_block
     | stmt+ T_GO?
     ;

// http://mail.hplsql.org/doc
// 1. doris statement include plsql stmt:
//      alter_table_stmt
//      create_database_stmt
//      create_index_stmt
//      create_table_stmt
//      delete_stmt
//      describe_stmt
//      insert_stmt
//      insert_directory_stmt
//      grant_stmt
//      select_stmt
//      update_stmt
//      use_stmt
//      truncate_stmt
//
// 2. TODO add plsql stmt:
//      begin_transaction_stmt
//      end_transaction_stmt
//      rollback_stmt
//      collect_stats_stmt
//      cmp_stmt
//      copy_from_local_stmt
//      copy_stmt
//      commit_stmt
//      create_local_temp_table_stmt
//      merge_stmt
//      summary_stmt
//
// 3. delete hplsql stmt
//      create_table_type_stmt
//      hive
doris_statement :
       statement
     ;

stmt :
       doris_statement
     | assignment_stmt
     | allocate_cursor_stmt
     | associate_locator_stmt
     | break_stmt
     | call_stmt
     | close_stmt
     | create_procedure_stmt
     | create_function_stmt // like a simple procedure
     | create_package_stmt
     | create_package_body_stmt
     | declare_stmt
     | exec_stmt
     | exit_stmt
     | fetch_stmt
     | for_cursor_stmt
     | for_range_stmt
     | if_stmt
     | include_stmt
     | get_diag_stmt
     | leave_stmt
     | map_object_stmt
     | open_stmt
     | print_stmt
     | quit_stmt
     | raise_stmt
     | resignal_stmt
     | return_stmt
     | signal_stmt
     | values_into_stmt
     | while_stmt
     | unconditional_loop_stmt
     | label
     | host_pl
     | null_stmt
     | expr_stmt
     | semicolon_stmt      // Placed here to allow null statements ;;...
     ;

semicolon_stmt :
       T_SEMICOLON
     | '@' | '#' | '/'
     ;

exception_block :       // Exception block
       T_EXCEPTION exception_block_item+
     ;

exception_block_item :
       T_WHEN L_ID T_THEN block ~(T_WHEN | T_END)
     ;

null_stmt :             // NULL statement (no operation)
       T_NULL
     ;

expr_stmt :             // Standalone expression
       {!_input.LT(1).getText().equalsIgnoreCase("GO")}? expr
     ;

assignment_stmt :       // Assignment statement
       T_SET set_session_option
     | T_SET? assignment_stmt_item (T_COMMA assignment_stmt_item)*
     ;

assignment_stmt_item :
       assignment_stmt_single_item
     | assignment_stmt_multiple_item
     | assignment_stmt_select_item
     | assignment_stmt_collection_item
     ;

assignment_stmt_single_item :
       ident_pl T_COLON? T_EQUAL expr
     | T_OPEN_P ident_pl T_CLOSE_P T_COLON? T_EQUAL expr
     ;

assignment_stmt_collection_item :
    expr_func T_COLON T_EQUAL expr
    ;

assignment_stmt_multiple_item :
       T_OPEN_P ident_pl (T_COMMA ident_pl)* T_CLOSE_P T_COLON? T_EQUAL T_OPEN_P expr (T_COMMA expr)* T_CLOSE_P
     ;

assignment_stmt_select_item :
       (ident_pl | (T_OPEN_P ident_pl (T_COMMA ident_pl)* T_CLOSE_P)) T_COLON? T_EQUAL T_OPEN_P query T_CLOSE_P
     ;

allocate_cursor_stmt:
       T_ALLOCATE ident_pl T_CURSOR T_FOR ((T_RESULT T_SET) | T_PROCEDURE) ident_pl
     ;

associate_locator_stmt :
       T_ASSOCIATE (T_RESULT T_SET)? (T_LOCATOR | T_LOCATORS) T_OPEN_P ident_pl (T_COMMA ident_pl)* T_CLOSE_P T_WITH T_PROCEDURE ident_pl
     ;

break_stmt :
       T_BREAK
     ;

call_stmt :
       T_CALL (expr_dot | expr_func | ident_pl)
     ;

declare_stmt :          // Declaration statement
       T_DECLARE declare_stmt_item (T_COMMA declare_stmt_item)*
     ;

declare_block :         // Declaration block
       T_DECLARE declare_stmt_item T_SEMICOLON (declare_stmt_item T_SEMICOLON)*
     ;

declare_block_inplace :
       declare_stmt_item T_SEMICOLON (declare_stmt_item T_SEMICOLON)*
     ;

declare_stmt_item :
       declare_cursor_item
     | declare_condition_item
     | declare_handler_item
     | declare_var_item
     ; // TODO declare_temporary_table_item

declare_var_item :
       ident_pl (T_COMMA ident_pl)* T_AS? dtype dtype_len? dtype_attr* dtype_default?
     | ident_pl T_CONSTANT T_AS? dtype dtype_len? dtype_default
     ;

declare_condition_item :    // Condition declaration
       ident_pl T_CONDITION
     ;

declare_cursor_item :      // Cursor declaration
       (T_CURSOR ident_pl | ident_pl T_CURSOR) (cursor_with_return | cursor_without_return)? (T_IS | T_AS | T_FOR) (query | expr )
     ;

cursor_with_return :
       T_WITH T_RETURN T_ONLY? (T_TO (T_CALLER | T_CLIENT))?
     ;

cursor_without_return :
       T_WITHOUT T_RETURN
     ;

declare_handler_item :     // Condition handler declaration
       (T_CONTINUE | T_EXIT) T_HANDLER T_FOR (T_SQLEXCEPTION | T_SQLWARNING | T_NOT T_FOUND | ident_pl) single_block_stmt
     ;

dtype :                  // Data types
       T_CHAR
     | T_CHARACTER
     | T_BIGINT
     | T_BINARY_DOUBLE
     | T_BINARY_FLOAT
     | T_BINARY_INTEGER
     | T_BIT
     | T_DATE
     | T_DATETIME
     | T_DEC
     | T_DECIMAL
     | T_DOUBLE T_PRECISION?
     | T_FLOAT
     | T_INT
     | T_INT2
     | T_INT4
     | T_INT8
     | T_INTEGER
     | T_NCHAR
     | T_NVARCHAR
     | T_NUMBER
     | T_NUMERIC
     | T_PLS_INTEGER
     | T_REAL
     | T_RESULT_SET_LOCATOR T_VARYING
     | T_SIMPLE_FLOAT
     | T_SIMPLE_DOUBLE
     | T_SIMPLE_INTEGER
     | T_SMALLINT
     | T_SMALLDATETIME
     | T_STRING
     | T_SYS_REFCURSOR
     | T_TIMESTAMP
     | T_TINYINT
     | T_VARCHAR
     | T_VARCHAR2
     | T_XML
     | qident ('%' (T_TYPE | T_ROWTYPE))?             // User-defined or derived data type
     ;

dtype_len :             // Data type length or size specification
       T_OPEN_P (L_INT | T_MAX) (T_CHAR | T_BYTE)? (T_COMMA L_INT)? T_CLOSE_P
     ;

dtype_attr :
       T_NOT? T_NULL
     | T_CHARACTER T_SET ident_pl
     | T_NOT? (T_CASESPECIFIC | T_CS)
     ;

dtype_default :
       T_COLON? T_EQUAL expr
     | T_WITH? T_DEFAULT expr?
     ;

create_function_stmt :
      (T_ALTER | T_CREATE (T_OR T_REPLACE)? | T_REPLACE)? T_FUNCTION ident_pl create_routine_params? create_function_return (T_AS | T_IS)? declare_block_inplace? single_block_stmt
    ;

create_function_return :
       (T_RETURN | T_RETURNS) dtype dtype_len?
     ;

create_package_stmt :
      (T_ALTER | T_CREATE (T_OR T_REPLACE)? | T_REPLACE)? T_PACKAGE ident_pl (T_AS | T_IS) package_spec T_END (ident_pl T_SEMICOLON)?
    ;

package_spec :
      package_spec_item T_SEMICOLON (package_spec_item T_SEMICOLON)*
    ;

package_spec_item :
      declare_stmt_item
    | T_FUNCTION ident_pl create_routine_params? create_function_return
    | (T_PROCEDURE | T_PROC) ident_pl create_routine_params?
    ;

create_package_body_stmt :
      (T_ALTER | T_CREATE (T_OR T_REPLACE)? | T_REPLACE)? T_PACKAGE T_BODY ident_pl (T_AS | T_IS) package_body T_END (ident_pl T_SEMICOLON)?
    ;

package_body :
      package_body_item T_SEMICOLON (package_body_item T_SEMICOLON)*
    ;

package_body_item :
      declare_stmt_item
    | create_function_stmt
    | create_procedure_stmt
    ;

create_procedure_stmt :
      (T_ALTER | T_CREATE (T_OR T_REPLACE)? | T_REPLACE)? (T_PROCEDURE | T_PROC) ident_pl create_routine_params? create_routine_options? (T_AS | T_IS)? declare_block_inplace? label? procedure_block (ident_pl T_SEMICOLON)?
    ;

create_routine_params :
       T_OPEN_P T_CLOSE_P
     | T_OPEN_P create_routine_param_item (T_COMMA create_routine_param_item)* T_CLOSE_P
     | {!_input.LT(1).getText().equalsIgnoreCase("IS") &&
        !_input.LT(1).getText().equalsIgnoreCase("AS") &&
        !(_input.LT(1).getText().equalsIgnoreCase("DYNAMIC") && _input.LT(2).getText().equalsIgnoreCase("RESULT"))
        }?
       create_routine_param_item (T_COMMA create_routine_param_item)*
     ;

create_routine_param_item :
       (T_IN | T_OUT | T_INOUT | T_IN T_OUT)? ident_pl dtype dtype_len? dtype_attr* dtype_default?
     | ident_pl (T_IN | T_OUT | T_INOUT | T_IN T_OUT)? dtype dtype_len? dtype_attr* dtype_default?
     ;

create_routine_options :
       create_routine_option+
     ;
create_routine_option :
       T_LANGUAGE T_SQL
     | T_SQL T_SECURITY (T_CREATOR | T_DEFINER | T_INVOKER | T_OWNER)
     | T_DYNAMIC? T_RESULT T_SETS L_INT
     ;

exec_stmt :             // EXEC, EXECUTE IMMEDIATE statement
       (T_EXEC | T_EXECUTE) T_IMMEDIATE? expr (T_OPEN_P expr_func_params T_CLOSE_P | expr_func_params)? (T_INTO L_ID (T_COMMA L_ID)*)? using_clause?
     ;

if_stmt :               // IF statement
       if_plsql_stmt
     | if_tsql_stmt
     | if_bteq_stmt
     ;

if_plsql_stmt :
       T_IF bool_expr T_THEN block elseif_block* else_block? T_END T_IF
     ;

if_tsql_stmt :
       T_IF bool_expr single_block_stmt (T_ELSE single_block_stmt)?
     ;

if_bteq_stmt :
       '.' T_IF bool_expr T_THEN single_block_stmt
     ;

elseif_block :
       (T_ELSIF | T_ELSEIF) bool_expr T_THEN block
     ;

else_block :
       T_ELSE block
     ;

include_stmt :          // INCLUDE statement
       T_INCLUDE (file_name | expr)
     ;

exit_stmt :
       T_EXIT L_ID? (T_WHEN bool_expr)?
     ;

get_diag_stmt :         // GET DIAGNOSTICS statement
       T_GET T_DIAGNOSTICS get_diag_stmt_item
     ;

get_diag_stmt_item :
       get_diag_stmt_exception_item
     | get_diag_stmt_rowcount_item
     ;

get_diag_stmt_exception_item :
       T_EXCEPTION L_INT qident T_EQUAL T_MESSAGE_TEXT
     ;

get_diag_stmt_rowcount_item :
       qident T_EQUAL T_ROW_COUNT
     ;

leave_stmt :
       T_LEAVE L_ID?
     ;

map_object_stmt :
       T_MAP T_OBJECT ident_pl (T_TO ident_pl)? (T_AT ident_pl)?
     ;

open_stmt :             // OPEN cursor statement
         T_OPEN ident_pl (T_FOR (query | expr))?
     ;

fetch_stmt :            // FETCH cursor statement
       T_FETCH T_FROM? ident_pl bulk_collect_clause? T_INTO ident_pl (T_COMMA ident_pl)* fetch_limit?
     ;

fetch_limit:
      T_LIMIT expr
     ;

close_stmt :            // CLOSE cursor statement
       T_CLOSE L_ID
     ;

print_stmt :            // PRINT statement
       T_PRINT expr
     | T_PRINT T_OPEN_P expr T_CLOSE_P
     ;

quit_stmt :
       '.'? T_QUIT expr?
     ;

raise_stmt :
       T_RAISE
     ;

resignal_stmt :         // RESIGNAL statement
       T_RESIGNAL (T_SQLSTATE T_VALUE? expr (T_SET T_MESSAGE_TEXT T_EQUAL expr)? )?
     ;

return_stmt :           // RETURN statement
       T_RETURN expr?
     ;

set_session_option :
       set_doris_session_option         // First position is matched first
     | set_current_schema_option
     | set_mssql_session_option
     | set_teradata_session_option
     ;

set_doris_session_option :
       (T_GLOBAL | T_LOCAL | T_SESSION)? ident_pl T_EQUAL ident_pl // hplsql, 这里的含义是啥，什么情况会用到这个，是因为放在第一位，所以每次都用么 // hplsql会set一些局部变量，和我们 set seesion veriable冲突 // 所有的set xxx=xxx都当成了session veriables处理 //放在第一位先匹配
      ; //

set_current_schema_option :
       ((T_CURRENT? T_SCHEMA) | T_CURRENT_SCHEMA) T_EQUAL? expr
     ;

set_mssql_session_option :
     ( T_ANSI_NULLS
     | T_ANSI_PADDING
     | T_NOCOUNT
     | T_QUOTED_IDENTIFIER
     | T_XACT_ABORT )
     (T_ON | T_OFF)
     ;

set_teradata_session_option :
       T_QUERY_BAND T_EQUAL (expr | T_NONE) T_UPDATE? T_FOR (T_TRANSACTION | T_SESSION)
     ;

signal_stmt :          // SIGNAL statement
       T_SIGNAL ident_pl
     ;

values_into_stmt :     // VALUES INTO statement
       T_VALUES T_OPEN_P? expr (T_COMMA expr)* T_CLOSE_P? T_INTO T_OPEN_P? ident_pl (T_COMMA ident_pl)* T_CLOSE_P?
     ;

while_stmt :            // WHILE loop statement
       T_WHILE bool_expr (T_DO | T_LOOP | T_THEN | T_BEGIN) block T_END (T_WHILE | T_LOOP)?
     ;

unconditional_loop_stmt : // LOOP .. END LOOP
       T_LOOP block T_END T_LOOP
     ;

for_cursor_stmt :       // FOR (cursor) statement
       T_FOR L_ID T_IN T_OPEN_P? query T_CLOSE_P? T_LOOP block T_END T_LOOP
     ;

for_range_stmt :        // FOR (Integer range) statement
       T_FOR L_ID T_IN T_REVERSE? expr T_DOT2 expr ((T_BY | T_STEP) expr)? T_LOOP block T_END T_LOOP
     ;

label :
       L_LABEL
     | T_LESS T_LESS L_ID T_GREATER T_GREATER
     ;

using_clause :          // USING var,... clause
       T_USING expr (T_COMMA expr)*
     ;

bulk_collect_clause :
       T_BULK T_COLLECT
     ;

bool_expr :                               // Boolean condition
       T_NOT? T_OPEN_P bool_expr T_CLOSE_P
     | bool_expr bool_expr_logical_operator bool_expr
     | bool_expr_atom
     ;

bool_expr_atom :
      bool_expr_unary
    | bool_expr_binary
    | expr
    ;

bool_expr_unary :
      expr T_IS T_NOT? T_NULL
    | expr T_BETWEEN expr T_AND expr
    ; // TODO T_NOT? T_EXISTS T_OPEN_P query T_CLOSE_P, bool_expr_single_in, bool_expr_multi_in

bool_expr_binary :
       expr bool_expr_binary_operator expr
     ;

bool_expr_logical_operator :
       T_AND
     | T_OR
     ;

bool_expr_binary_operator :
       T_EQUAL
     | T_EQUAL2
     | T_NOTEQUAL
     | T_NOTEQUAL2
     | T_LESS
     | T_LESSEQUAL
     | T_GREATER
     | T_GREATEREQUAL
     | T_NOT? (T_LIKE | T_RLIKE | T_REGEXP)
     ;

expr :
       expr interval_item
     | expr (T_MUL | T_DIV) expr
     | expr (T_ADD | T_SUB) expr
     | T_OPEN_P query T_CLOSE_P
     | T_OPEN_P expr T_CLOSE_P
     | expr_interval
     | expr_concat
     | expr_dot
     | expr_case
     | expr_cursor_attribute
     | expr_agg_window_func
     | expr_spec_func
     | expr_func
     | expr_atom
     ;

expr_atom :
       date_literal
     | timestamp_literal
     | bool_literal
     | qident
     | string
     | dec_number
     | int_number
     | null_const
     ;

expr_interval :
       T_INTERVAL expr interval_item
     ;
interval_item :
       T_DAY
     | T_DAYS
     | T_MICROSECOND
     | T_MICROSECONDS
     | T_SECOND
     | T_SECONDS
     ;

expr_concat :                  // String concatenation operator
       expr_concat_item (T_PIPE | T_CONCAT) expr_concat_item ((T_PIPE | T_CONCAT) expr_concat_item)*
     ;

expr_concat_item :
       T_OPEN_P expr T_CLOSE_P
     | expr_case
     | expr_agg_window_func
     | expr_spec_func
     | expr_dot
     | expr_func
     | expr_atom
     ;

expr_case :                    // CASE expression
       expr_case_simple
     | expr_case_searched
     ;

expr_case_simple :
       T_CASE expr (T_WHEN expr T_THEN expr)+ (T_ELSE expr)? T_END
     ;

expr_case_searched :
       T_CASE (T_WHEN bool_expr T_THEN expr)+ (T_ELSE expr)? T_END
     ;

expr_cursor_attribute :
      ident_pl '%' (T_ISOPEN | T_FOUND | T_NOTFOUND)
    ;

expr_agg_window_func :
       T_AVG T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     | T_COUNT T_OPEN_P ((expr_func_all_distinct? expr) | '*') T_CLOSE_P expr_func_over_clause?
     | T_COUNT_BIG T_OPEN_P ((expr_func_all_distinct? expr) | '*') T_CLOSE_P expr_func_over_clause?
     | T_CUME_DIST T_OPEN_P T_CLOSE_P expr_func_over_clause
     | T_DENSE_RANK T_OPEN_P T_CLOSE_P expr_func_over_clause
     | T_FIRST_VALUE T_OPEN_P expr T_CLOSE_P expr_func_over_clause
     | T_LAG T_OPEN_P expr (T_COMMA expr (T_COMMA expr)?)? T_CLOSE_P expr_func_over_clause
     | T_LAST_VALUE T_OPEN_P expr T_CLOSE_P expr_func_over_clause
     | T_LEAD T_OPEN_P expr (T_COMMA expr (T_COMMA expr)?)? T_CLOSE_P expr_func_over_clause
     | T_MAX T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     | T_MIN T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     | T_RANK T_OPEN_P T_CLOSE_P expr_func_over_clause
     | T_ROW_NUMBER T_OPEN_P T_CLOSE_P expr_func_over_clause
     | T_STDEV T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     | T_SUM T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     | T_VAR T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     | T_VARIANCE T_OPEN_P expr_func_all_distinct? expr T_CLOSE_P expr_func_over_clause?
     ;

expr_func_all_distinct :
       T_ALL
     | T_DISTINCT
     ;

order_by_clause :
       T_ORDER T_BY expr (T_ASC | T_DESC)? (T_COMMA expr (T_ASC | T_DESC)?)*
     ;

expr_func_over_clause :
       T_OVER T_OPEN_P expr_func_partition_by_clause? order_by_clause? T_CLOSE_P
     ;

expr_func_partition_by_clause :
       T_PARTITION T_BY expr (T_COMMA expr)*
     ;

expr_spec_func :
       T_ACTIVITY_COUNT
     | T_CAST T_OPEN_P expr T_AS  dtype dtype_len? T_CLOSE_P
     | T_COUNT T_OPEN_P (expr | '*') T_CLOSE_P
     | T_CURRENT_DATE | T_CURRENT T_DATE
     | (T_CURRENT_TIMESTAMP | T_CURRENT T_TIMESTAMP) (T_OPEN_P expr T_CLOSE_P)?
     | T_CURRENT_USER | T_CURRENT T_USER
     | T_MAX_PART_STRING T_OPEN_P expr (T_COMMA expr (T_COMMA expr T_EQUAL expr)*)? T_CLOSE_P
     | T_MIN_PART_STRING T_OPEN_P expr (T_COMMA expr (T_COMMA expr T_EQUAL expr)*)? T_CLOSE_P
     | T_MAX_PART_INT T_OPEN_P expr (T_COMMA expr (T_COMMA expr T_EQUAL expr)*)? T_CLOSE_P
     | T_MIN_PART_INT T_OPEN_P expr (T_COMMA expr (T_COMMA expr T_EQUAL expr)*)? T_CLOSE_P
     | T_MAX_PART_DATE T_OPEN_P expr (T_COMMA expr (T_COMMA expr T_EQUAL expr)*)? T_CLOSE_P
     | T_MIN_PART_DATE T_OPEN_P expr (T_COMMA expr (T_COMMA expr T_EQUAL expr)*)? T_CLOSE_P
     | T_PART_COUNT T_OPEN_P expr (T_COMMA expr T_EQUAL expr)* T_CLOSE_P
     | T_PART_LOC T_OPEN_P expr (T_COMMA expr T_EQUAL expr)+ (T_COMMA expr)? T_CLOSE_P
     | T_TRIM T_OPEN_P expr T_CLOSE_P
     | T_SUBSTRING T_OPEN_P expr T_FROM expr (T_FOR expr)? T_CLOSE_P
     | T_SYSDATE
     | T_USER
     ;

expr_func :
       ident_pl T_OPEN_P expr_func_params? T_CLOSE_P
     ;

expr_dot :
       expr_dot_method_call | expr_dot_property_access
      ;

expr_dot_method_call :
       (ident_pl | expr_func) T_DOT expr_func
      ;

expr_dot_property_access :
       (ident_pl | expr_func) T_DOT ident_pl
      ;

expr_func_params :
       func_param (T_COMMA func_param)*
     ;

func_param :
       {!_input.LT(1).getText().equalsIgnoreCase("INTO")}? (ident_pl T_EQUAL T_GREATER?)? expr
     ;

host_pl :
       '!' host_cmd  ';'                   // OS command
     | host_stmt
     ;

host_cmd :
       .*?
     ;

host_stmt :
       T_HOST expr
     ;

file_name :
       L_FILE | ('/' | '.' '/')? qident ('/' qident)*
     ;

date_literal :                             // DATE 'YYYY-MM-DD' literal
       T_DATE string
     ;

timestamp_literal :                       // TIMESTAMP 'YYYY-MM-DD HH:MI:SS.FFF' literal
       T_TIMESTAMP string
     ;

ident_pl :
       '-'? (L_ID | non_reserved_words)
     ;

qident :                                  // qualified identifier e.g: table_name.col_name or db_name._table_name
       ident_pl ('.'ident_pl)*
     ;

string :                                   // String literal (single or double quoted)
       L_S_STRING                          # single_quotedString
     | L_D_STRING                          # double_quotedString
     ;

int_number :                               // Integer (positive or negative)
     ('-' | '+')? L_INT
     ;

dec_number :                               // Decimal number (positive or negative)
     ('-' | '+')? L_DEC
     ;

bool_literal :                            // Boolean literal
       T_TRUE
     | T_FALSE
     ;

null_const :                              // NULL constant
       T_NULL
     ;

non_reserved_words :                      // Tokens that are not reserved words and can be used as identifiers
       T_ACTION
     | T_ACTIVITY_COUNT
     | T_ADD2
     | T_ALL
     | T_ALLOCATE
     | T_ALTER
     | T_AND
     | T_ANSI_NULLS
     | T_ANSI_PADDING
     | T_AS
     | T_ASC
     | T_ASSOCIATE
     | T_AT
     | T_AUTO_INCREMENT
     | T_AVG
     | T_BATCHSIZE
     | T_BEGIN
     | T_BETWEEN
     | T_BIGINT
     | T_BINARY_DOUBLE
     | T_BINARY_FLOAT
     | T_BIT
     | T_BODY
     | T_BREAK
     | T_BULK
     | T_BY
     | T_BYTE
     | T_CALL
     | T_CALLER
     | T_CASCADE
     | T_CASE
     | T_CASESPECIFIC
     | T_CAST
     | T_CHAR
     | T_CHARACTER
     | T_CHARSET
     | T_CLIENT
     | T_CLOSE
     | T_CLUSTERED
     | T_CMP
     | T_COLLECT
     | T_COLLECTION
     | T_COLUMN
     | T_COMMENT
     | T_COMPRESS
     | T_CONSTANT
     | T_COPY
     | T_COMMIT
     | T_CONCAT
     | T_CONDITION
     | T_CONSTRAINT
     | T_CONTINUE
     | T_COUNT
     | T_COUNT_BIG
     | T_CREATE
     | T_CREATION
     | T_CREATOR
     | T_CS
     | T_CUME_DIST
     | T_CURRENT
     | T_CURRENT_DATE
     | T_CURRENT_SCHEMA
     | T_CURRENT_TIMESTAMP
     | T_CURRENT_USER
     | T_CURSOR
     | T_DATA
     | T_DATABASE
     | T_DATE
     | T_DATETIME
     | T_DAY
     | T_DAYS
     | T_DEC
     | T_DECIMAL
     | T_DECLARE
     | T_DEFAULT
     | T_DEFERRED
     | T_DEFINED
     | T_DEFINER
     | T_DEFINITION
     | T_DELETE
     | T_DELIMITED
     | T_DELIMITER
     | T_DENSE_RANK
     | T_DESC
     | T_DESCRIBE
     | T_DIAGNOSTICS
     | T_DIR
     | T_DIRECTORY
     | T_DISTINCT
     | T_DISTRIBUTE
     | T_DO
     | T_DOUBLE
     | T_DROP
     | T_DYNAMIC
     // T_ELSE reserved word
     // T_ELSEIF reserved word
     // T_ELSIF reserved word
     // T_END reserved word
     | T_ENABLE
     | T_ENGINE
     | T_ESCAPED
     | T_EXCEPT
     | T_EXEC
     | T_EXECUTE
     | T_EXCEPTION
     | T_EXCLUSIVE
     | T_EXISTS
     | T_EXIT
     | T_FALLBACK
     | T_FALSE
     | T_FETCH
     | T_FIELDS
     | T_FILE
     | T_FILES
     | T_FIRST_VALUE
     | T_FLOAT
     | T_FOR
     | T_FOREIGN
     | T_FORMAT
     | T_FOUND
     | T_FROM
     | T_FULL
     | T_FUNCTION
     | T_GET
     | T_GLOBAL
     | T_GO
     | T_GRANT
     | T_GROUP
     | T_HANDLER
     | T_HASH
     | T_HAVING
     | T_HDFS
     | T_HIVE
     | T_HOST
     | T_IDENTITY
     | T_IF
     | T_IGNORE
     | T_IMMEDIATE
     | T_IN
     | T_INCLUDE
     | T_INDEX
     | T_INITRANS
     | T_INNER
     | T_INOUT
     | T_INSERT
     | T_INT
     | T_INT2
     | T_INT4
     | T_INT8
     | T_INTEGER
     | T_INTERSECT
     | T_INTERVAL
     | T_INTO
     | T_INVOKER
     | T_ITEMS
     | T_IS
     | T_ISOPEN
     | T_JOIN
     | T_KEEP
     | T_KEY
     | T_KEYS
     | T_LAG
     | T_LANGUAGE
     | T_LAST_VALUE
     | T_LEAD
     | T_LEAVE
     | T_LEFT
     | T_LIKE
     | T_LIMIT
     | T_LINES
     | T_LOCAL
     | T_LOCATION
     | T_LOCATOR
     | T_LOCATORS
     | T_LOCKS
     | T_LOG
     | T_LOGGED
     | T_LOGGING
     | T_LOOP
     | T_MAP
     | T_MATCHED
     | T_MAX
     | T_MAXTRANS
     | T_MERGE
     | T_MESSAGE_TEXT
     | T_MICROSECOND
     | T_MICROSECONDS
     | T_MIN
     | T_MULTISET
     | T_NCHAR
     | T_NEW
     | T_NVARCHAR
     | T_NO
     | T_NOCOMPRESS
     | T_NOCOUNT
     | T_NOLOGGING
     | T_NONE
     | T_NOT
     | T_NOTFOUND
     // T_NULL reserved word
     | T_NUMERIC
     | T_NUMBER
     | T_OBJECT
     | T_OFF
     | T_ON
     | T_ONLY
     | T_OPEN
     | T_OR
     | T_ORDER
     | T_OUT
     | T_OUTER
     | T_OVER
     | T_OVERWRITE
     | T_OWNER
     | T_PACKAGE
     | T_PART_COUNT
     | T_PART_LOC
     | T_PARTITION
     | T_PCTFREE
     | T_PCTUSED
     | T_PRECISION
     | T_PRESERVE
     | T_PRIMARY
     | T_PRINT
     | T_PROC
     | T_PROCEDURE
     | T_PWD
     | T_QUALIFY
     | T_QUERY_BAND
     | T_QUIT
     | T_QUOTED_IDENTIFIER
     | T_RAISE
     | T_RANK
     | T_REAL
     | T_REFERENCES
     | T_REGEXP
     | T_RR
     | T_REPLACE
     | T_RESIGNAL
     | T_RESTRICT
     | T_RESULT
     | T_RESULT_SET_LOCATOR
     | T_RETURN
     | T_RETURNS
     | T_REVERSE
     | T_RIGHT
     | T_RLIKE
     | T_RS
     | T_ROLE
     | T_ROLLBACK
     | T_ROW
     | T_ROWS
     | T_ROW_COUNT
     | T_ROW_NUMBER
     | T_SCHEMA
     | T_SECOND
     | T_SECONDS
     | T_SECURITY
     | T_SEGMENT
     | T_SEL
     | T_SELECT
     | T_SESSION
     | T_SESSIONS
     | T_SET
     | T_SETS
     | T_SHARE
     | T_SIGNAL
     | T_SIMPLE_DOUBLE
     | T_SIMPLE_FLOAT
     | T_SMALLDATETIME
     | T_SMALLINT
     | T_SQL
     | T_SQLEXCEPTION
     | T_SQLINSERT
     | T_SQLSTATE
     | T_SQLWARNING
     | T_STATS
     | T_STATISTICS
     | T_STEP
     | T_STDEV
     | T_STORAGE
     | T_STORED
     | T_STRING
     | T_SUBDIR
     | T_SUBSTRING
     | T_SUM
     | T_SUMMARY
     | T_SYSDATE
     | T_SYS_REFCURSOR
     | T_TABLE
     | T_TABLESPACE
     | T_TEMPORARY
     | T_TERMINATED
     | T_TEXTIMAGE_ON
     | T_THEN
     | T_TIMESTAMP
     | T_TITLE
     | T_TO
     | T_TOP
     | T_TRANSACTION
     | T_TRIM
     | T_TRUE
     | T_TRUNCATE
     // T_UNION reserved word
     | T_UNIQUE
     | T_UPDATE
     | T_UR
     | T_USE
     | T_USER
     | T_USING
     | T_VALUE
     | T_VALUES
     | T_VAR
     | T_VARCHAR
     | T_VARCHAR2
     | T_VARYING
     | T_VARIANCE
     | T_VOLATILE
     // T_WHEN reserved word
     // T_WHERE reserved word
     | T_WHILE
     | T_WITH
     | T_WITHOUT
     | T_WORK
     | T_XACT_ABORT
     | T_XML
     | T_YES
     ;

// Lexer rules
T_ACTION          : A C T I O N ;
T_ADD2            : A D D ;
T_ALL             : A L L ;
T_ALLOCATE        : A L L O C A T E ;
T_ALTER           : A L T E R ;
T_AND             : A N D ;
T_ANSI_NULLS      : A N S I '_' N U L L S ;
T_ANSI_PADDING    : A N S I '_' P A D D I N G ;
T_AS              : A S ;
T_ASC             : A S C ;
T_ASSOCIATE       : A S S O C I A T E ;
T_AT              : A T ;
T_AUTO_INCREMENT  : A U T O '_' I N C R E M E N T ;
T_AVG             : A V G ;
T_BATCHSIZE       : B A T C H S I Z E ;
T_BEGIN           : B E G I N ;
T_BETWEEN         : B E T W E E N ;
T_BIGINT          : B I G I N T ;
T_BINARY_DOUBLE   : B I N A R Y '_' D O U B L E ;
T_BINARY_FLOAT    : B I N A R Y '_' F L O A T ;
T_BINARY_INTEGER  : B I N A R Y '_' I N T E G E R ;
T_BIT             : B I T ;
T_BODY            : B O D Y ;
T_BREAK           : B R E A K ;
T_BULK            : B U L K ;
T_BY              : B Y ;
T_BYTE            : B Y T E ;
T_CALL            : C A L L ;
T_CALLER          : C A L L E R ;
T_CASCADE         : C A S C A D E ;
T_CASE            : C A S E ;
T_CASESPECIFIC    : C A S E S P E C I F I C ;
T_CAST            : C A S T ;
T_CHAR            : C H A R ;
T_CHARACTER       : C H A R A C T E R ;
T_CHARSET         : C H A R S E T ;
T_CLIENT          : C L I E N T ;
T_CLOSE           : C L O S E ;
T_CLUSTERED       : C L U S T E R E D;
T_CMP             : C M P ;
T_COLLECT         : C O L L E C T ;
T_COLLECTION      : C O L L E C T I O N ;
T_COLUMN          : C O L U M N ;
T_COMMENT         : C O M M E N T;
T_CONSTANT        : C O N S T A N T ;
T_COMMIT          : C O M M I T ;
T_COMPRESS        : C O M P R E S S ;
T_CONCAT          : C O N C A T;
T_CONDITION       : C O N D I T I O N ;
T_CONSTRAINT      : C O N S T R A I N T ;
T_CONTINUE        : C O N T I N U E ;
T_COPY            : C O P Y ;
T_COUNT           : C O U N T ;
T_COUNT_BIG       : C O U N T '_' B I G;
T_CREATE          : C R E A T E ;
T_CREATION        : C R E A T I O N ;
T_CREATOR         : C R E A T O R ;
T_CS              : C S;
T_CURRENT         : C U R R E N T ;
T_CURRENT_SCHEMA  : C U R R E N T '_' S C H E M A ;
T_CURSOR          : C U R S O R ;
T_DATABASE        : D A T A B A S E ;
T_DATA            : D A T A ;
T_DATE            : D A T E ;
T_DATETIME        : D A T E T I M E ;
T_DAY             : D A Y ;
T_DAYS            : D A Y S ;
T_DEC             : D E C ;
T_DECIMAL         : D E C I M A L ;
T_DECLARE         : D E C L A R E ;
T_DEFAULT         : D E F A U L T ;
T_DEFERRED        : D E F E R R E D ;
T_DEFINED         : D E F I N E D ;
T_DEFINER         : D E F I N E R ;
T_DEFINITION      : D E F I N I T I O N ;
T_DELETE          : D E L E T E ;
T_DELIMITED       : D E L I M I T E D ;
T_DELIMITER       : D E L I M I T E R ;
T_DESC            : D E S C ;
T_DESCRIBE        : D E S C R I B E ;
T_DIAGNOSTICS     : D I A G N O S T I C S ;
T_DIR             : D I R ;
T_DIRECTORY       : D I R E C T O R Y ;
T_DISTINCT        : D I S T I N C T ;
T_DISTRIBUTE      : D I S T R I B U T E ;
T_DO              : D O ;
T_DOUBLE          : D O U B L E ;
T_DROP            : D R O P ;
T_DYNAMIC         : D Y N A M I C ;
T_ELSE            : E L S E ;
T_ELSEIF          : E L S E I F ;
T_ELSIF           : E L S I F ;
T_ENABLE          : E N A B L E ;
T_END             : E N D ;
T_ENGINE          : E N G I N E ;
T_ESCAPED         : E S C A P E D ;
T_EXCEPT          : E X C E P T ;
T_EXEC            : E X E C ;
T_EXECUTE         : E X E C U T E ;
T_EXCEPTION       : E X C E P T I O N ;
T_EXCLUSIVE       : E X C L U S I V E ;
T_EXISTS          : E X I S T S ;
T_EXIT            : E X I T ;
T_FALLBACK        : F A L L B A C K ;
T_FALSE           : F A L S E ;
T_FETCH           : F E T C H ;
T_FIELDS          : F I E L D S ;
T_FILE            : F I L E ;
T_FILES           : F I L E S ;
T_FLOAT           : F L O A T ;
T_FOR             : F O R ;
T_FOREIGN         : F O R E I G N ;
T_FORMAT          : F O R M A T ;
T_FOUND           : F O U N D ;
T_FROM            : F R O M ;
T_FULL            : F U L L ;
T_FUNCTION        : F U N C T I O N ;
T_GET             : G E T ;
T_GLOBAL          : G L O B A L ;
T_GO              : G O ;
T_GRANT           : G R A N T ;
T_GROUP           : G R O U P ;
T_HANDLER         : H A N D L E R ;
T_HASH            : H A S H ;
T_HAVING          : H A V I N G ;
T_HDFS            : H D F S ;
T_HIVE            : H I V E ;
T_HOST            : H O S T ;
T_IDENTITY        : I D E N T I T Y ;
T_IF              : I F ;
T_IGNORE          : I G N O R E ;
T_IMMEDIATE       : I M M E D I A T E ;
T_IN              : I N ;
T_INCLUDE         : I N C L U D E ;
T_INDEX           : I N D E X ;
T_INITRANS        : I N I T R A N S ;
T_INNER           : I N N E R ;
T_INOUT           : I N O U T;
T_INSERT          : I N S E R T ;
T_INT             : I N T ;
T_INT2            : I N T '2';
T_INT4            : I N T '4';
T_INT8            : I N T '8';
T_INTEGER         : I N T E G E R ;
T_INTERSECT       : I N T E R S E C T ;
T_INTERVAL        : I N T E R V A L ;
T_INTO            : I N T O ;
T_INVOKER         : I N V O K E R ;
T_IS              : I S ;
T_ISOPEN          : I S O P E N ;
T_ITEMS           : I T E M S ;
T_JOIN            : J O I N ;
T_KEEP            : K E E P;
T_KEY             : K E Y ;
T_KEYS            : K E Y S ;
T_LANGUAGE        : L A N G U A G E ;
T_LEAVE           : L E A V E ;
T_LEFT            : L E F T ;
T_LIKE            : L I K E ;
T_LIMIT           : L I M I T ;
T_LINES           : L I N E S ;
T_LOCAL           : L O C A L ;
T_LOCATION        : L O C A T I O N ;
T_LOCATOR         : L O C A T O R ;
T_LOCATORS        : L O C A T O R S ;
T_LOCKS           : L O C K S ;
T_LOG             : L O G ;
T_LOGGED          : L O G G E D ;
T_LOGGING         : L O G G I N G ;
T_LOOP            : L O O P ;
T_MAP             : M A P ;
T_MATCHED         : M A T C H E D ;
T_MAX             : M A X ;
T_MAXTRANS        : M A X T R A N S ;
T_MERGE           : M E R G E ;
T_MESSAGE_TEXT    : M E S S A G E '_' T E X T ;
T_MICROSECOND     : M I C R O S E C O N D ;
T_MICROSECONDS    : M I C R O S E C O N D S;
T_MIN             : M I N ;
T_MULTISET        : M U L T I S E T ;
T_NCHAR           : N C H A R ;
T_NEW             : N E W ;
T_NVARCHAR        : N V A R C H A R ;
T_NO              : N O ;
T_NOCOUNT         : N O C O U N T ;
T_NOCOMPRESS      : N O C O M P R E S S ;
T_NOLOGGING       : N O L O G G I N G ;
T_NONE            : N O N E ;
T_NOT             : N O T ;
T_NOTFOUND        : N O T F O U N D ;
T_NULL            : N U L L ;
T_NUMERIC         : N U M E R I C ;
T_NUMBER          : N U M B E R ;
T_OBJECT          : O B J E C T ;
T_OFF             : O F F ;
T_OF              : O F ;
T_ON              : O N ;
T_ONLY            : O N L Y ;
T_OPEN            : O P E N ;
T_OR              : O R ;
T_ORDER           : O R D E R;
T_OUT             : O U T ;
T_OUTER           : O U T E R ;
T_OVER            : O V E R ;
T_OVERWRITE       : O V E R W R I T E ;
T_OWNER           : O W N E R ;
T_PACKAGE         : P A C K A G E ;
T_PARTITION       : P A R T I T I O N ;
T_PCTFREE         : P C T F R E E ;
T_PCTUSED         : P C T U S E D ;
T_PLS_INTEGER     : P L S '_' I N T E G E R ;
T_PRECISION       : P R E C I S I O N ;
T_PRESERVE        : P R E S E R V E ;
T_PRIMARY         : P R I M A R Y ;
T_PRINT           : P R I N T ;
T_PROC            : P R O C ;
T_PROCEDURE       : P R O C E D U R E ;
T_QUALIFY         : Q U A L I F Y ;
T_QUERY_BAND      : Q U E R Y '_' B A N D ;
T_QUIT            : Q U I T ;
T_QUOTED_IDENTIFIER : Q U O T E D '_' I D E N T I F I E R ;
T_RAISE           : R A I S E ;
T_REAL            : R E A L ;
T_REFERENCES      : R E F E R E N C E S ;
T_REGEXP          : R E G E X P ;
T_REPLACE         : R E P L A C E ;
T_RESIGNAL        : R E S I G N A L ;
T_RESTRICT        : R E S T R I C T ;
T_RESULT          : R E S U L T ;
T_RESULT_SET_LOCATOR : R E S U L T '_' S E T '_' L O C A T O R ;
T_RETURN          : R E T U R N ;
T_RETURNS         : R E T U R N S ;
T_REVERSE         : R E V E R S E ;
T_RIGHT           : R I G H T ;
T_RLIKE           : R L I K E ;
T_ROLE            : R O L E ;
T_ROLLBACK        : R O L L B A C K ;
T_ROW             : R O W ;
T_ROWS            : R O W S ;
T_ROWTYPE         : R O W T Y P E ;
T_ROW_COUNT       : R O W '_' C O U N T ;
T_RR              : R R;
T_RS              : R S ;
T_PWD             : P W D ;
T_TRIM            : T R I M ;
T_SCHEMA          : S C H E M A ;
T_SECOND          : S E C O N D ;
T_SECONDS         : S E C O N D S;
T_SECURITY        : S E C U R I T Y ;
T_SEGMENT         : S E G M E N T ;
T_SEL             : S E L ;
T_SELECT          : S E L E C T ;
T_SET             : S E T ;
T_SESSION         : S E S S I O N ;
T_SESSIONS        : S E S S I O N S ;
T_SETS            : S E T S;
T_SHARE           : S H A R E ;
T_SIGNAL          : S I G N A L ;
T_SIMPLE_DOUBLE   : S I M P L E '_' D O U B L E ;
T_SIMPLE_FLOAT    : S I M P L E '_' F L O A T ;
T_SIMPLE_INTEGER  : S I M P L E '_' I N T E G E R ;
T_SMALLDATETIME   : S M A L L D A T E T I M E ;
T_SMALLINT        : S M A L L I N T ;
T_SQL             : S Q L ;
T_SQLEXCEPTION    : S Q L E X C E P T I O N ;
T_SQLINSERT       : S Q L I N S E R T ;
T_SQLSTATE        : S Q L S T A T E ;
T_SQLWARNING      : S Q L W A R N I N G ;
T_STATS           : S T A T S ;
T_STATISTICS      : S T A T I S T I C S ;
T_STEP            : S T E P ;
T_STORAGE         : S T O R A G E ;
T_STORED          : S T O R E D ;
T_STRING          : S T R I N G ;
T_SUBDIR          : S U B D I R ;
T_SUBSTRING       : S U B S T R I N G ;
T_SUM             : S U M ;
T_SUMMARY         : S U M M A R Y ;
T_SYS_REFCURSOR   : S Y S '_' R E F C U R S O R ;
T_TABLE           : T A B L E ;
T_TABLESPACE      : T A B L E S P A C E ;
T_TEMPORARY       : T E M P O R A R Y ;
T_TERMINATED      : T E R M I N A T E D ;
T_TEXTIMAGE_ON    : T E X T I M A G E '_' O N ;
T_THEN            : T H E N ;
T_TIMESTAMP       : T I M E S T A M P ;
T_TINYINT         : T I N Y I N T ;
T_TITLE           : T I T L E ;
T_TO              : T O ;
T_TOP             : T O P ;
T_TRANSACTION     : T R A N S A C T I O N ;
T_TRUE            : T R U E ;
T_TRUNCATE        : T R U N C A T E;
T_TYPE            : T Y P E ;
T_UNION           : U N I O N ;
T_UNIQUE          : U N I Q U E ;
T_UPDATE          : U P D A T E ;
T_UR              : U R ;
T_USE             : U S E ;
T_USING           : U S I N G ;
T_VALUE           : V A L U E ;
T_VALUES          : V A L U E S ;
T_VAR             : V A R ;
T_VARCHAR         : V A R C H A R ;
T_VARCHAR2        : V A R C H A R '2' ;
T_VARYING         : V A R Y I N G ;
T_VOLATILE        : V O L A T I L E ;
T_WHEN            : W H E N ;
T_WHERE           : W H E R E ;
T_WHILE           : W H I L E ;
T_WITH            : W I T H ;
T_WITHOUT         : W I T H O U T ;
T_WORK            : W O R K ;
T_XACT_ABORT      : X A C T '_' A B O R T ;
T_XML             : X M L ;
T_YES             : Y E S ;

// Functions with specific syntax
T_ACTIVITY_COUNT       : A C T I V I T Y '_' C O U N T ;
T_CUME_DIST            : C U M E '_' D I S T ;
T_CURRENT_DATE         : C U R R E N T '_' D A T E ;
T_CURRENT_TIMESTAMP    : C U R R E N T '_' T I M E S T A M P ;
T_CURRENT_USER         : C U R R E N T '_' U S E R ;
T_DENSE_RANK           : D E N S E '_' R A N K ;
T_FIRST_VALUE          : F I R S T '_' V A L U E;
T_LAG                  : L A G ;
T_LAST_VALUE           : L A S T '_' V A L U E;
T_LEAD                 : L E A D ;
T_MAX_PART_STRING      : M A X '_' P A R T '_' S T R I N G ;
T_MIN_PART_STRING      : M I N '_' P A R T '_' S T R I N G ;
T_MAX_PART_INT         : M A X '_' P A R T '_' I N T ;
T_MIN_PART_INT         : M I N '_' P A R T '_' I N T ;
T_MAX_PART_DATE        : M A X '_' P A R T '_' D A T E ;
T_MIN_PART_DATE        : M I N '_' P A R T '_' D A T E ;
T_PART_COUNT           : P A R T '_' C O U N T ;
T_PART_LOC             : P A R T '_' L O C ;
T_RANK                 : R A N K ;
T_ROW_NUMBER           : R O W '_' N U M B E R;
T_STDEV                : S T D E V ;
T_SYSDATE              : S Y S D A T E ;
T_VARIANCE             : V A R I A N C E ;
T_USER                 : U S E R;

T_ADD          : '+' ;
T_COLON        : ':' ;
T_COMMA        : ',' ;
T_PIPE         : '||' ;
T_DIV          : '/' ;
T_DOT          : '.' ;
T_DOT2         : '..' ;
T_EQUAL        : '=' ;
T_EQUAL2       : '==' ;
T_NOTEQUAL     : '<>' ;
T_NOTEQUAL2    : '!=' ;
T_GREATER      : '>' ;
T_GREATEREQUAL : '>=' ;
T_LESS         : '<' ;
T_LESSEQUAL    : '<=' ;
T_MUL          : '*' ;
T_OPEN_B       : '{' ;
T_OPEN_P       : '(' ;
T_OPEN_SB      : '[' ;
T_CLOSE_B      : '}' ;
T_CLOSE_P      : ')' ;
T_CLOSE_SB     : ']' ;
T_SEMICOLON    : ';' ;
T_SUB          : '-' ;

L_ID        : L_ID_PART                                                // Identifier
            ;
L_S_STRING  : '\'' (('\'' '\'') | ('\\' '\'') | ~('\''))* '\''         // Single quoted string literal
            ;
L_D_STRING  : '"' (L_STR_ESC_D | .)*? '"'                              // Double quoted string literal
            ;
L_INT       : L_DIGIT+ ;                                               // Integer
L_DEC       : L_DIGIT+ '.' ~'.' L_DIGIT*                               // Decimal number
            | '.' L_DIGIT+
            ;
L_WS        : L_BLANK+ -> skip ;                                       // Whitespace
L_M_COMMENT : '/*' .*? '*/' -> channel(HIDDEN) ;                       // Multiline comment
L_S_COMMENT : ('--' | '//')  .*? '\r'? '\n' -> channel(HIDDEN) ;       // Single line comment

L_FILE      : ([a-zA-Z] ':' '\\'?)? L_ID ('\\' L_ID)*                  // File path (a/b/c Linux path causes conflicts with division operator and handled at parser level)
            ;

L_LABEL     : ([a-zA-Z] | L_DIGIT | '_')* ':'
            ;

fragment
L_ID_PART  :
             [a-zA-Z] ([a-zA-Z] | L_DIGIT | '_')*                           // Identifier part
            | '$' '{' .*? '}'
            | ('_' | '@' | ':' | '#' | '$') ([a-zA-Z] | L_DIGIT | '_' | '@' | ':' | '#' | '$')+     // (at least one char must follow special char)
            | '"' .*? '"'                                                   // Quoted identifiers
            | '[' .*? ']'
            | '`' .*? '`'
            ;
fragment
L_STR_ESC_D :                                                          // Double quoted string escape sequence
              '""' | '\\"'
            ;
fragment
L_DIGIT     : [0-9]                                                    // Digit
            ;
fragment
L_BLANK     : (' ' | '\t' | '\r' | '\n')
            ;

// Support case-insensitive keywords and allowing case-sensitive identifiers
fragment A : ('a'|'A') ;
fragment B : ('b'|'B') ;
fragment C : ('c'|'C') ;
fragment D : ('d'|'D') ;
fragment E : ('e'|'E') ;
fragment F : ('f'|'F') ;
fragment G : ('g'|'G') ;
fragment H : ('h'|'H') ;
fragment I : ('i'|'I') ;
fragment J : ('j'|'J') ;
fragment K : ('k'|'K') ;
fragment L : ('l'|'L') ;
fragment M : ('m'|'M') ;
fragment N : ('n'|'N') ;
fragment O : ('o'|'O') ;
fragment P : ('p'|'P') ;
fragment Q : ('q'|'Q') ;
fragment R : ('r'|'R') ;
fragment S : ('s'|'S') ;
fragment T : ('t'|'T') ;
fragment U : ('u'|'U') ;
fragment V : ('v'|'V') ;
fragment W : ('w'|'W') ;
fragment X : ('x'|'X') ;
fragment Y : ('y'|'Y') ;
fragment Z : ('z'|'Z') ;
