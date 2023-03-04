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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'aes_encrypt'. This class is generated by GenerateFunction.
 */
public class AesEncrypt extends AesCryptoFunction {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT,
                            VarcharType.SYSTEM_DEFAULT,
                            VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT,
                            VarcharType.SYSTEM_DEFAULT,
                            VarcharType.SYSTEM_DEFAULT,
                            VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE)
    );

    /**
     * Some javadoc for checkstyle...
     */
    public AesEncrypt(Expression arg0, Expression arg1) {
        super("aes_encrypt", arg0, arg1, getDefaultBlockEncryptionMode());
        String blockEncryptionMode = String.valueOf(getDefaultBlockEncryptionMode());
        if (!blockEncryptionMode.toUpperCase().equals("AES_128_ECB")
                && !blockEncryptionMode.toUpperCase().equals("AES_192_ECB")
                && !blockEncryptionMode.toUpperCase().equals("AES_256_ECB")) {
            throw new AnalysisException("Incorrect parameter count in the call to native function "
                    + "'aes_encrypt' or 'aes_decrypt'");
        }
    }

    public AesEncrypt(Expression arg0, Expression arg1, Expression arg2) {
        super("aes_encrypt", arg0, arg1, arg2, getDefaultBlockEncryptionMode());
    }

    public AesEncrypt(Expression arg0, Expression arg1, Expression arg2, Expression arg3) {
        super("aes_encrypt", arg0, arg1, arg2, arg3);
    }

    /**
     * withChildren.
     */
    @Override
    public AesEncrypt withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2 && children.size() <= 4);
        if (children.size() == 2) {
            return new AesEncrypt(children.get(0), children.get(1));
        } else if (children().size() == 3) {
            return new AesEncrypt(children.get(0), children.get(1), children.get(2));
        } else {
            if (!(children.get(3) instanceof StringLiteral)) {
                throw new AnalysisException("the 4th parameter should be string literal: " + this.toSql());
            }
            return new AesEncrypt(children.get(0), children.get(1), children.get(2), (StringLiteral) children.get(3));
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAesEncrypt(this, context);
    }
}
