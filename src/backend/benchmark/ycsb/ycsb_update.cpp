//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.cpp
//
// Identification: benchmark/tpcc/workload.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <iostream>
#include <ctime>
#include <cassert>
#include <thread>
#include <algorithm>
#include <random>
#include <cstddef>
#include <limits>

#include "backend/benchmark/ycsb/ycsb_workload.h"
#include "backend/benchmark/ycsb/ycsb_configuration.h"
#include "backend/benchmark/ycsb/ycsb_loader.h"

#include "backend/catalog/manager.h"
#include "backend/catalog/schema.h"

#include "backend/common/types.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/logger.h"
#include "backend/common/timer.h"
#include "backend/common/generator.h"

#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/concurrency/transaction_scheduler.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/executor/materialization_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/index_scan_executor.h"
#include "backend/executor/insert_executor.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/container_tuple.h"

#include "backend/index/index_factory.h"

#include "backend/logging/log_manager.h"

#include "backend/planner/abstract_plan.h"
#include "backend/planner/materialization_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/update_plan.h"
#include "backend/planner/index_scan_plan.h"

#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"

namespace peloton {
namespace benchmark {
namespace ycsb {

/////////////////////////////////////////////////////////////////////////////////////////
// Queue Based YCSB methods
/////////////////////////////////////////////////////////////////////////////////////////

// void SetUpdate(UpdatePlans &update_plans, ZipfDistribution &zipf) {
//  std::unique_ptr<executor::ExecutorContext> context(
//      new executor::ExecutorContext(nullptr));
//
//  update_plans.SetContext(context.get());
//  update_plans.ResetState();
//
//  std::vector<Value> values;
//
//  auto lookup_key = zipf.GetNextNumber();
//
//  values.push_back(ValueFactory::GetIntegerValue(lookup_key));
//
//  update_plans.index_scan_executor_->SetValues(values);
//
//  TargetList target_list;
//  // std::string update_raw_value(ycsb_field_length - 1, 'u');
//  int update_raw_value = 2;
//
//  Value update_val = ValueFactory::GetIntegerValue(update_raw_value);
//
//  target_list.emplace_back(
//      1, expression::ExpressionUtil::ConstantValueFactory(update_val));
//
//  update_plans.update_executor_->SetTargetList(target_list);
//}

void SetUpdate(UpdateQuery *&query, ZipfDistribution &zipf) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  query->SetContext(context.get());
  query->ResetState();

  std::vector<Value> values;

  auto lookup_key = zipf.GetNextNumber();

  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  query->GetIndexScanExecutor()->SetValues(values);

  TargetList target_list;
  // std::string update_raw_value(ycsb_field_length - 1, 'u');
  int update_raw_value = 2;

  Value update_val = ValueFactory::GetIntegerValue(update_raw_value);

  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  query->GetUpdateExecutor()->SetTargetList(target_list);
}

UpdateQuery *GenerateAndQueueUpdate(ZipfDistribution &zipf) {

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////
  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);
  std::vector<Value> values;
  std::vector<expression::AbstractExpression *> runtime_keys;
  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor
  planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                         index_scan_desc);

  // TODO: Should delete after executing the query
  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(&index_scan_node, nullptr);

  /////////////////////////////////////////////////////////
  // Generate UPDATE: plan , executor , query
  /////////////////////////////////////////////////////////

  TargetList target_list;
  Value update_val = ValueFactory::GetIntegerValue(2);
  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  DirectMapList direct_map_list;
  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  // Generate update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan *update_node =
      new planner::UpdatePlan(user_table, std::move(project_info));

  // Generate update executor
  executor::UpdateExecutor *update_executor =
      new executor::UpdateExecutor(update_node, nullptr);

  update_executor->AddChild(index_scan_executor);
  update_executor->Init();

  // Generate update query
  UpdateQuery *query =
      new UpdateQuery(index_scan_executor, update_executor, update_node);

  /////////////////////////////////////////////////////////
  // Set UPDATE query (executor)
  /////////////////////////////////////////////////////////
  // SetUpdate(query, zipf);
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  query->SetContext(context.get());
  query->ResetState();

  std::vector<Value> values2;
  auto lookup_key = zipf.GetNextNumber();
  values2.push_back(ValueFactory::GetIntegerValue(lookup_key));

  query->GetIndexScanExecutor()->SetValues(values2);

  /////////////////////////////////////////////////////////
  // Call txn scheduler to queue this executor
  /////////////////////////////////////////////////////////

  // Push the query into the queue
  // Note: when poping the query and after executing it, the update_executor and
  // index_executor should be deleted, then query itself should be deleted
  // concurrency::TransactionScheduler::GetInstance().SimpleEnqueue(query);
  return query;
}

// void DestroyUpdateQuery(concurrency::TransactionQuery *query) {
//  executor::UpdateExecutor *update_executor =
//      reinterpret_cast<executor::UpdateExecutor *>(query->GetExecutor());
//
//  executor::IndexScanExecutor *index_sexecutor =
//      reinterpret_cast<executor::IndexScanExecutor *>(
//          (update_executor->GetChildren()).front());
//
//  delete index_sexecutor;
//  delete update_executor;
//}

/////////////////////////////////////////////////////////////////////////////////////////
// Non-queued YCSB
/////////////////////////////////////////////////////////////////////////////////////////

UpdatePlans PrepareUpdatePlan() {

  /////////////////////////////////////////////////////////
  // INDEX SCAN + PREDICATE
  /////////////////////////////////////////////////////////

  std::vector<oid_t> key_column_ids;
  std::vector<ExpressionType> expr_types;
  key_column_ids.push_back(0);
  expr_types.push_back(ExpressionType::EXPRESSION_TYPE_COMPARE_EQUAL);

  std::vector<Value> values;

  std::vector<expression::AbstractExpression *> runtime_keys;

  auto ycsb_pkey_index = user_table->GetIndexWithOid(user_table_pkey_index_oid);

  planner::IndexScanPlan::IndexScanDesc index_scan_desc(
      ycsb_pkey_index, key_column_ids, expr_types, values, runtime_keys);

  // Create plan node.
  auto predicate = nullptr;

  // Column ids to be added to logical tile after scan.
  std::vector<oid_t> column_ids;
  oid_t column_count = state.column_count + 1;

  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    column_ids.push_back(col_itr);
  }

  // Create and set up index scan executor
  planner::IndexScanPlan index_scan_node(user_table, predicate, column_ids,
                                         index_scan_desc);

  executor::IndexScanExecutor *index_scan_executor =
      new executor::IndexScanExecutor(&index_scan_node, nullptr);

  /////////////////////////////////////////////////////////
  // UPDATE
  /////////////////////////////////////////////////////////

  TargetList target_list;
  DirectMapList direct_map_list;

  // Update the second attribute
  for (oid_t col_itr = 0; col_itr < column_count; col_itr++) {
    if (col_itr != 1) {
      direct_map_list.emplace_back(col_itr,
                                   std::pair<oid_t, oid_t>(0, col_itr));
    }
  }

  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(user_table, std::move(project_info));

  executor::UpdateExecutor *update_executor =
      new executor::UpdateExecutor(&update_node, nullptr);

  update_executor->AddChild(index_scan_executor);

  update_executor->Init();

  UpdatePlans update_plans;

  update_plans.index_scan_executor_ = index_scan_executor;

  update_plans.update_executor_ = update_executor;

  return update_plans;
}

bool RunUpdate(UpdatePlans &update_plans, ZipfDistribution &zipf) {

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(nullptr));

  update_plans.SetContext(context.get());
  update_plans.ResetState();

  std::vector<Value> values;

  auto lookup_key = zipf.GetNextNumber();

  values.push_back(ValueFactory::GetIntegerValue(lookup_key));

  update_plans.index_scan_executor_->SetValues(values);

  TargetList target_list;
  // std::string update_raw_value(ycsb_field_length - 1, 'u');
  int update_raw_value = 2;

  Value update_val = ValueFactory::GetIntegerValue(update_raw_value);

  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));

  update_plans.update_executor_->SetTargetList(target_list);

  /////////////////////////////////////////////////////////
  // EXECUTE
  /////////////////////////////////////////////////////////

  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Execute the query
  ExecuteUpdateTest(update_plans.update_executor_);

  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  // transaction passed execution.
  assert(txn->GetResult() == Result::RESULT_SUCCESS);

  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    return true;

  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    return false;
  }
}
}
}
}
