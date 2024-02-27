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
// https://github.com/apache/hive/blob/master/hplsql/src/main/java/org/apache/hive/hplsql/UndefinedIdentException.java
// and modified by Doris

package org.apache.doris.plsql.exception;

import org.antlr.v4.runtime.ParserRuleContext;

public class UndefinedIdentException extends PlValidationException {
    public UndefinedIdentException(ParserRuleContext ctx, String ident) {
        super(ctx, "identifier '" + ident + "' must be declared.");
    }
}
