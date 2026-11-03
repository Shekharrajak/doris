#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
ARCHIVE_DIR="${1:?Usage: $0 directory-containing-the-pinned-librdkafka-archive}"
ARCHIVE_DIR="$(cd "${ARCHIVE_DIR}" && pwd)"
TP_DIR="${ROOT}"
source "${ROOT}/vars.sh"
MARKER="patched_mark_message_topic_id_v1"
SOURCE_FILES=(src/rdkafka.h src/rdkafka_fetcher.c src/rdkafka_msg.c src/rdkafka_op.h
    tests/0006-symbols.c tests/0156-share_consumer_fetch_mock.c tests/0184-share_consumer_topic_recreate.c)

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

[[ -r "${ARCHIVE_DIR}/${LIBRDKAFKA_NAME}" ]] || fail "missing pinned archive ${LIBRDKAFKA_NAME}"
if command -v md5sum >/dev/null 2>&1; then
    archive_md5="$(md5sum "${ARCHIVE_DIR}/${LIBRDKAFKA_NAME}" | awk '{ print $1 }')"
else
    archive_md5="$(md5 -q "${ARCHIVE_DIR}/${LIBRDKAFKA_NAME}")"
fi
[[ "${archive_md5}" == "${LIBRDKAFKA_MD5SUM}" ]] || fail "pinned archive checksum mismatch"
mkdir -p "${ROOT}/src"
tmpdir="$(mktemp -d "${ROOT}/src/librdkafka-message-topic-id-test.XXXXXX")"
trap 'rm -rf "${tmpdir}"' EXIT

prepare() {
    local dest="$1"
    mkdir -p "${dest}/src"
    cp "${ROOT}/vars.sh" "${dest}/vars.sh"
    ln -s "${ROOT}/patches" "${dest}/patches"
    cp "${ARCHIVE_DIR}/${LIBRDKAFKA_NAME}" "${dest}/src/"
}

run_download() {
    TP_DIR="$1" DORIS_HOME="${tmpdir}" bash "${ROOT}/download-thirdparty.sh" librdkafka
}

has() {
    grep -Fq "$3" "$1/src/${LIBRDKAFKA_SOURCE}/$2" || fail "missing $3 in $2"
}

check_sources() {
    has "$1" src/rdkafka.h 'rd_kafka_message_topic_id(const rd_kafka_message_t *rkmessage);'
    has "$1" src/rdkafka_fetcher.c 'msg_rko->rko_u.fetch.topic_id ='
    has "$1" src/rdkafka_fetcher.c 'parpriv->topic_id;'
    has "$1" src/rdkafka_msg.c 'return &rko->rko_u.fetch.topic_id;'
    has "$1" src/rdkafka_op.h 'rd_kafka_Uuid_t topic_id;'
    has "$1" tests/0006-symbols.c 'rd_kafka_message_topic_id(NULL);'
    has "$1" tests/0156-share_consumer_fetch_mock.c 'do_test_message_topic_id();'
    has "$1" tests/0184-share_consumer_topic_recreate.c 'Delivered topic ID differs from '
    has "$1" tests/0184-share_consumer_topic_recreate.c 'Borrowed topic ID changed after topic recreation'
    has "$1" tests/0184-share_consumer_topic_recreate.c 'Retained message resolved the recreated topic ID'
    [[ -f "$1/src/${LIBRDKAFKA_SOURCE}/patched_mark" ]] || fail "missing legacy patch marker"
    [[ -f "$1/src/${LIBRDKAFKA_SOURCE}/${MARKER}" ]] || fail "missing identity patch marker"
}

fingerprint() {
    (cd "$1/src/${LIBRDKAFKA_SOURCE}" && cksum "${SOURCE_FILES[@]}")
}

prepare "${tmpdir}/fresh"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
fingerprint "${tmpdir}/fresh" > "${tmpdir}/fingerprint"
touch "${tmpdir}/fresh/src/${LIBRDKAFKA_SOURCE}/cache_reuse_sentinel"
run_download "${tmpdir}/fresh"
check_sources "${tmpdir}/fresh"
[[ -f "${tmpdir}/fresh/src/${LIBRDKAFKA_SOURCE}/cache_reuse_sentinel" ]] \
    || fail "unchanged patch replaced cached sources"
fingerprint "${tmpdir}/fresh" > "${tmpdir}/rerun-fingerprint"
cmp "${tmpdir}/fingerprint" "${tmpdir}/rerun-fingerprint"
echo "PASS: fresh archive and idempotent identity patch"

prepare "${tmpdir}/cached"
tar xzf "${ARCHIVE_DIR}/${LIBRDKAFKA_NAME}" -C "${tmpdir}/cached/src"
cached_source="${tmpdir}/cached/src/${LIBRDKAFKA_SOURCE}"
(cd "${cached_source}" && patch -p0 < "${ROOT}/patches/librdkafka-2.16.0.patch")
touch "${cached_source}/patched_mark" "${cached_source}/legacy_cache_sentinel"
run_download "${tmpdir}/cached"
check_sources "${tmpdir}/cached"
[[ -f "${cached_source}/legacy_cache_sentinel" ]] || fail "legacy cached sources were replaced"
fingerprint "${tmpdir}/cached" > "${tmpdir}/cached-fingerprint"
cmp "${tmpdir}/fingerprint" "${tmpdir}/cached-fingerprint"
echo "PASS: legacy patched sources receive the identity patch"

prepare "${tmpdir}/invalid"
tar xzf "${ARCHIVE_DIR}/${LIBRDKAFKA_NAME}" -C "${tmpdir}/invalid/src"
# A missing target makes the identity patch fail against the real upstream tree.
rm "${tmpdir}/invalid/src/${LIBRDKAFKA_SOURCE}/src/rdkafka.h"
if run_download "${tmpdir}/invalid" > "${tmpdir}/invalid.log" 2>&1; then
    fail "expected an incompatible source to reject the identity patch"
fi
[[ -f "${tmpdir}/invalid/src/${LIBRDKAFKA_SOURCE}/patched_mark" ]] \
    || fail "failure occurred before the identity patch"
[[ ! -f "${tmpdir}/invalid/src/${LIBRDKAFKA_SOURCE}/${MARKER}" ]] \
    || fail "failed identity patch was marked complete"
echo "PASS: identity patch failure does not mark sources ready"
