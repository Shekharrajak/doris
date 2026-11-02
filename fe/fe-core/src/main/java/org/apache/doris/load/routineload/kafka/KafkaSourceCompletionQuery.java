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

package org.apache.doris.load.routineload.kafka;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public final class KafkaSourceCompletionQuery {
    private KafkaSourceCompletionQuery() {
    }

    public static Optional<KafkaSourceDescriptor> getCompleted(Env env, long dbId, long transactionId, long jobId,
            UUID taskId, KafkaSourceDescriptor source) throws UserException {
        if (source.getNamespaceId() != jobId) {
            throw new UserException("Kafka source namespace does not match the routine load job");
        }
        Database database = env.getInternalCatalog().getDbOrMetaException(dbId);
        RoutineLoadJob job = env.getRoutineLoadManager().getJob(jobId);
        if (!(job instanceof KafkaRoutineLoadJob) || !((KafkaRoutineLoadJob) job).isUseShareConsumer()
                || job.isMultiTable() || job.getDbId() != dbId) {
            throw new UserException("Kafka source completion query requires a single-table Kafka share job");
        }
        long tableId = job.getTableId();
        database.getTableOrMetaException(tableId);
        TransactionState transaction = env.getGlobalTransactionMgr().getTransactionState(dbId, transactionId);
        if (transaction == null) {
            throw new UserException("Kafka source completion transaction does not exist");
        }
        synchronized (transaction) {
            List<Long> tableIds = transaction.getTableIdList();
            if (transaction.getTransactionId() != transactionId || transaction.getDbId() != dbId
                    || transaction.getSourceType() != LoadJobSourceType.ROUTINE_LOAD_TASK
                    || transaction.getTransactionStatus() != TransactionStatus.PREPARE
                    || transaction.getCallbackId() != jobId || transaction.getSubTxnIds() != null
                    || tableIds.size() != 1 || tableIds.get(0) != tableId) {
                throw new UserException("Kafka source completion transaction does not belong to the routine load job");
            }
        }
        if (!job.containsRunningTask(taskId, transactionId)) {
            throw new UserException("Kafka source completion task is no longer running in the routine load job");
        }
        return env.getKafkaSourceCompletionLedger().getCompleted(source);
    }
}
