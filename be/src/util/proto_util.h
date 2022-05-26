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

#pragma once

#include "util/md5.h"
#include "util/stack_util.h"

namespace doris {

// 2G * 0.9
// 2G:
// 0.9:
constexpr size_t MIN_HTTP_BRPC_SIZE = (1ULL << 5) * 0.9;

// TODO(zxy) delete in 1.2 version
// Transfer RowBatch in ProtoBuf Request to Controller Attachment.
// This can avoid reaching the upper limit of the ProtoBuf Request length (2G),
// and it is expected that performance can be improved.
template <typename Params, typename Closure>
inline void request_row_batch_transfer_attachment(Params* brpc_request,
                                                  const std::string& tuple_data, Closure* closure) {
    auto row_batch = brpc_request->mutable_row_batch();
    row_batch->set_tuple_data("");
    brpc_request->set_transfer_by_attachment(true);
    butil::IOBuf attachment;
    attachment.append(tuple_data);
    closure->cntl.request_attachment().swap(attachment);
}

// TODO(zxy) delete in 1.2 version
// Transfer Block in ProtoBuf Request to Controller Attachment.
// This can avoid reaching the upper limit of the ProtoBuf Request length (2G),
// and it is expected that performance can be improved.
template <typename Params, typename Closure>
inline void request_block_transfer_attachment(Params* brpc_request,
                                              const std::string& column_values, Closure* closure) {
    auto block = brpc_request->mutable_block();
    block->set_column_values("");
    brpc_request->set_transfer_by_attachment(true);
    butil::IOBuf attachment;
    attachment.append(column_values);
    closure->cntl.request_attachment().swap(attachment);
}

// TODO(zxy) delete in 1.2 version
// Controller Attachment transferred to RowBatch in ProtoBuf Request.
template <typename Params>
inline void attachment_transfer_request_row_batch(const Params* brpc_request,
                                                  brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    if (req->has_row_batch() && req->transfer_by_attachment()) {
        auto rb = req->mutable_row_batch();
        const butil::IOBuf& io_buf = cntl->request_attachment();
        CHECK(io_buf.size() > 0) << io_buf.size() << ", row num: " << req->row_batch().num_rows();
        io_buf.copy_to(rb->mutable_tuple_data(), io_buf.size(), 0);
    }
}

// TODO(zxy) delete in 1.2 version
// Controller Attachment transferred to Block in ProtoBuf Request.
template <typename Params>
inline void attachment_transfer_request_block(const Params* brpc_request, brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    if (req->has_block() && req->transfer_by_attachment()) {
        auto block = req->mutable_block();
        const butil::IOBuf& io_buf = cntl->request_attachment();
        CHECK(io_buf.size() > 0) << io_buf.size();
        io_buf.copy_to(block->mutable_column_values(), io_buf.size(), 0);
    }
}

// Transfer RowBatch in ProtoBuf Request to Controller Attachment.
// This can avoid reaching the upper limit of the ProtoBuf Request length (2G),
// and it is expected that performance can be improved.
template <typename Params, typename Closure>
inline void request_embed_attachment_contain_row_batch(Params* brpc_request,
                                                       const std::string& tuple_data,
                                                       Closure* closure) {
    auto row_batch = brpc_request->mutable_row_batch();
    row_batch->set_tuple_data("");
    request_embed_attachment(brpc_request, tuple_data, closure);
}

// Transfer Block in ProtoBuf Request to Controller Attachment.
// This can avoid reaching the upper limit of the ProtoBuf Request length (2G),
// and it is expected that performance can be improved.
template <typename Params, typename Closure>
inline void request_embed_attachment_contain_block(Params* brpc_request,
                                                   const std::string& column_values,
                                                   Closure* closure) {
    auto block = brpc_request->mutable_block();
    block->set_column_values("");
    request_embed_attachment(brpc_request, column_values, closure);
}

template <typename Params, typename Closure>
inline void request_embed_attachment(Params* brpc_request, const std::string& data,
                                     Closure* closure) {
    LOG(WARNING) << "http_brpc, request_embed_attachment start, data.size: " << data.size()
                 << std::endl;
    butil::IOBuf attachment;

    // step1: serialize brpc_request to string, and append to attachment.
    std::string req_str;
    brpc_request->SerializeToString(&req_str);
    int64_t req_str_size = req_str.size();
    attachment.append(&req_str_size, sizeof(req_str_size));
    attachment.append(req_str);

    // step2: append data to attachment and put it in the closure.
    int64_t data_size = data.size();
    attachment.append(&data_size, sizeof(data_size));
    attachment.append(data);

    // step3: attachment add to closure.
    closure->cntl.request_attachment().swap(attachment);

    // TODO delete it
    Md5Digest digest;
    digest.update(req_str.data(), req_str.size());
    digest.digest();
    Md5Digest digest1;
    digest1.update(data.data(), data.size());
    digest1.digest();
    LOG(WARNING) << "http_brpc, send attachment.size(): "
                 << closure->cntl.request_attachment().size()
                 << ", sizeof(req_str_size): " << sizeof(req_str_size)
                 << ", req_str.size: " << req_str_size << ", req_str: " << digest.hex()
                 << ", data.size: " << data_size << ", data: " << digest1.hex() << std::endl;
}

// Controller Attachment transferred to RowBatch in ProtoBuf Request.
template <typename Params>
inline void attachment_extract_request_contain_row_batch(const Params* brpc_request,
                                                         brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    auto rb = req->mutable_row_batch();
    attachment_extract_request(req, cntl, rb->mutable_tuple_data());
}

// Controller Attachment transferred to Block in ProtoBuf Request.
template <typename Params>
inline void attachment_extract_request_contain_block(const Params* brpc_request,
                                                     brpc::Controller* cntl) {
    Params* req = const_cast<Params*>(brpc_request);
    auto block = req->mutable_block();
    attachment_extract_request(req, cntl, block->mutable_column_values());
}

template <typename Params>
inline void attachment_extract_request(const Params* brpc_request, brpc::Controller* cntl,
                                       std::string* data) {
    const butil::IOBuf& io_buf = cntl->request_attachment();
    LOG(WARNING) << "http_brpc, attachment_extract_request start, io_buf.size: " << io_buf.size()
                 << std::endl;

    // step1: deserialize request string to brpc_request from attachment.
    int64_t req_str_size;
    io_buf.copy_to(&req_str_size, sizeof(req_str_size), 0);
    std::string req_str;
    io_buf.copy_to(&req_str, req_str_size, sizeof(req_str_size));
    Params* req = const_cast<Params*>(brpc_request);
    req->ParseFromString(req_str);

    // step2: extract data from attachment.
    int64_t data_size;
    io_buf.copy_to(&data_size, sizeof(data_size), sizeof(req_str_size) + req_str_size);
    io_buf.copy_to(data, data_size, sizeof(data_size) + sizeof(req_str_size) + req_str_size);

    // TODO: delete it
    Md5Digest digest;
    digest.update(req_str.data(), req_str_size);
    digest.digest();
    Md5Digest digest1;
    digest1.update(data->data(), data_size);
    digest1.digest();
    LOG(WARNING) << "http_brpc, revice io_buf.size(): " << io_buf.size()
                 << ", req_str.size: " << req_str_size << ", req_str " << digest.hex()
                 << ", data.size: " << data_size << ", data " << digest1.hex() << std::endl;
}

} // namespace doris
