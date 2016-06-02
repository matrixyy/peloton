//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// ycsb_workload.cpp
//
// Identification: benchmark/ycsb/ycsb_workload.cpp
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

#include "backend/expression/abstract_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/expression_util.h"

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

/////////////////////////////////////////////////////////
// WORKLOAD
/////////////////////////////////////////////////////////

volatile bool is_running = true;

oid_t *abort_counts;
oid_t *commit_counts;
oid_t *generate_counts;

// Helper function to pin current thread to a specific core
static void PinToCore(size_t core) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core, &cpuset);
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void RunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto update_ratio = state.update_ratio;

  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];

  ZipfDistribution zipf(state.scale_factor * 1000 - 1, state.zipf_theta);

  // Run these many transactions
  if (state.run_mix) {

    MixedPlans mixed_plans = PrepareMixedPlan();

    int write_count = 10 * update_ratio;
    int read_count = 10 - write_count;

    // backoff
    uint32_t backoff_shifts = 0;
    while (true) {
      if (is_running == false) {
        break;
      }
      while (RunMixed(mixed_plans, zipf, read_count, write_count) == false) {
        execution_count_ref++;
        // backoff
        if (state.run_backoff) {
          if (backoff_shifts < 63) {
            ++backoff_shifts;
          }
          uint64_t spins = 1UL << backoff_shifts;
          spins *= 100;
          while (spins) {
            _mm_pause();
            --spins;
          }
        }
      }
      backoff_shifts >>= 1;

      transaction_count_ref++;
    }
  } else {

    fast_random rng(rand());
    ReadPlans read_plans = PrepareReadPlan();
    UpdatePlans update_plans = PrepareUpdatePlan();
    // backoff
    uint32_t backoff_shifts = 0;
    while (true) {
      if (is_running == false) {
        break;
      }
      auto rng_val = rng.next_uniform();

      if (rng_val < update_ratio) {
        while (RunUpdate(update_plans, zipf) == false) {
          execution_count_ref++;
          // backoff
          if (state.run_backoff) {
            if (backoff_shifts < 63) {
              ++backoff_shifts;
            }
            uint64_t spins = 1UL << backoff_shifts;
            spins *= 100;
            while (spins) {
              _mm_pause();
              --spins;
            }
          }
        }
      } else {
        while (RunRead(read_plans, zipf) == false) {
          execution_count_ref++;
          // backoff
          if (state.run_backoff) {
            if (backoff_shifts < 63) {
              ++backoff_shifts;
            }
            uint64_t spins = 1UL << backoff_shifts;
            spins *= 100;
            while (spins) {
              _mm_pause();
              --spins;
            }
          }
        }
      }
      transaction_count_ref++;
    }
  }
}

void RunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;

  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);

  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);

  size_t snapshot_round = (size_t)(state.duration / state.snapshot_duration);

  oid_t **abort_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    abort_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  oid_t **commit_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    commit_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  // Launch a group of threads
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(RunBackend, thread_itr)));
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
    memcpy(abort_counts_snapshots[round_id], abort_counts,
           sizeof(oid_t) * num_threads);
    memcpy(commit_counts_snapshots[round_id], commit_counts,
           sizeof(oid_t) * num_threads);
  }

  is_running = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // calculate the throughput and abort rate for the first round.
  oid_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[0][i];
  }

  oid_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[0][i];
  }

  state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                      state.snapshot_duration);
  state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                      total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < snapshot_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_snapshots[round_id + 1][i] -
                            commit_counts_snapshots[round_id][i];
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_snapshots[round_id + 1][i] -
                           abort_counts_snapshots[round_id][i];
    }

    state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                        state.snapshot_duration);
    state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                        total_commit_count);
  }

  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[snapshot_round - 1][i];
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[snapshot_round - 1][i];
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  // cleanup everything.
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] abort_counts_snapshots[round_id];
    abort_counts_snapshots[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] commit_counts_snapshots[round_id];
    commit_counts_snapshots[round_id] = nullptr;
  }
  delete[] abort_counts_snapshots;
  abort_counts_snapshots = nullptr;
  delete[] commit_counts_snapshots;
  commit_counts_snapshots = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;
}

/////////////////////////////////////////////////////////
// HARNESS
/////////////////////////////////////////////////////////

std::vector<std::vector<Value>> ExecuteReadTest(
    executor::AbstractExecutor *executor) {

  std::vector<std::vector<Value>> logical_tile_values;

  // Execute stuff
  while (executor->Execute() == true) {
    std::unique_ptr<executor::LogicalTile> result_tile(executor->GetOutput());

    // is this possible?
    if (result_tile == nullptr) break;

    auto column_count = result_tile->GetColumnCount();

    for (oid_t tuple_id : *result_tile) {
      expression::ContainerTuple<executor::LogicalTile> cur_tuple(
          result_tile.get(), tuple_id);
      std::vector<Value> tuple_values;
      for (oid_t column_itr = 0; column_itr < column_count; column_itr++) {
        auto value = cur_tuple.GetValue(column_itr);
        tuple_values.push_back(value);
      }

      // Move the tuple list
      logical_tile_values.push_back(std::move(tuple_values));
    }
  }

  return std::move(logical_tile_values);
}

void ExecuteUpdateTest(executor::AbstractExecutor *executor) {

  // Execute stuff
  while (executor->Execute() == true)
    ;
}

// If return true and query is null, that means the queue is empty
// If return true and query is not null, that means execute successfully
// If return false, that means execute fail
bool PopAndExecuteQuery(concurrency::TransactionQuery *&ret_query) {
  /////////////////////////////////////////////////////////
  // EXECUTE : Get a query from the queue and execute it
  /////////////////////////////////////////////////////////

  // Start a txn to execute the query
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Get a query from a queue
  concurrency::TransactionQuery *query = nullptr;
  concurrency::TransactionScheduler::GetInstance().SimpleDequeue(query);

  // Return true
  if (query == nullptr) {
    ret_query = nullptr;
    return true;
  }

  peloton::PlanNodeType plan_type = query->GetPlanType();

  // Execute the query
  switch (plan_type) {

    case PLAN_NODE_TYPE_UPDATE: {
      ExecuteUpdateTest(
          reinterpret_cast<UpdateQuery *>(query)->GetUpdateExecutor());
      break;
    }

    default: {
      LOG_INFO("plan_type :: Unsupported Plan Tag: %u ", plan_type);
      elog(INFO, "Query: ");
      break;
    }
  }

  /////////////////////////////////////////////////////////
  // Transaction fail
  /////////////////////////////////////////////////////////
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    ret_query = query;
    return false;
  }

  /////////////////////////////////////////////////////////
  // Transaction Commit
  /////////////////////////////////////////////////////////
  assert(txn->GetResult() == Result::RESULT_SUCCESS);
  auto result = txn_manager.CommitTransaction();

  if (result == Result::RESULT_SUCCESS) {
    ret_query = query;
    return true;

  } else {
    // transaction failed commitment.
    assert(result == Result::RESULT_ABORTED ||
           result == Result::RESULT_FAILURE);
    ret_query = query;
    return false;
  }
}

bool ExecuteQuery(concurrency::TransactionQuery *query) {
  // Start a txn to execute the query
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  // Execute the query
  assert(query != nullptr);
  peloton::PlanNodeType plan_type = query->GetPlanType();
  switch (plan_type) {
    case PLAN_NODE_TYPE_UPDATE: {
      ExecuteUpdateTest(
          reinterpret_cast<UpdateQuery *>(query)->GetUpdateExecutor());
      break;
    }
    default: {
      LOG_INFO("plan_type :: Unsupported Plan Tag: %u ", plan_type);
      elog(INFO, "Query: ");
      break;
    }
  }

  /////////////////////////////////////////////////////////
  // Transaction fail
  /////////////////////////////////////////////////////////
  if (txn->GetResult() != Result::RESULT_SUCCESS) {
    txn_manager.AbortTransaction();
    return false;
  }

  /////////////////////////////////////////////////////////
  // Transaction success
  /////////////////////////////////////////////////////////
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
void QueryBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto update_ratio = state.update_ratio;
  oid_t &generate_count_ref = generate_counts[thread_id - state.backend_count];
  ZipfDistribution zipf(state.scale_factor * 1000 - 1, state.zipf_theta);
  fast_random rng(rand());

  while (true) {
    if (is_running == false) {
      break;
    }
    auto rng_val = rng.next_uniform();
    // Generate Update into queue
    if (rng_val < update_ratio) {
      GenerateAndQueueUpdate(zipf);
    } else {
      GenerateAndQueueUpdate(zipf);
    }
    generate_count_ref++;
  }
}

void PopQuery(oid_t thread_id) {
  PinToCore(thread_id);
  oid_t &transaction_count_ref = commit_counts[thread_id];

  while (true) {
    if (is_running == false) {
      break;
    }
    // Get a query from a queue
    concurrency::TransactionQuery *query = nullptr;
    concurrency::TransactionScheduler::GetInstance().SimpleDequeue(query);
    if (query == nullptr) {
      continue;
    }
    transaction_count_ref++;
    reinterpret_cast<UpdateQuery *>(query)->Cleanup();
    delete query;
  }
}
void ExecuteBackend(oid_t thread_id) {
  PinToCore(thread_id);
  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];

  while (true) {
    if (is_running == false) {
      break;
    }
    // Pop a query from a queue and execute
    // Execute a query
    concurrency::TransactionQuery *ret_query;

    //////////////////////////////////////////
    // Execute fail : retry
    //////////////////////////////////////////
    if (PopAndExecuteQuery(ret_query) == false) {
      execution_count_ref++;
      // std::cout << "execution_count_ref: " << execution_count_ref <<
      // std::endl;
      if (is_running == false) {
        break;
      }
      // retry to execute the failed query
      while (ExecuteQuery(ret_query) == false) {
        std::cout << "execution_count_ref: " << execution_count_ref
                  << std::endl;
        execution_count_ref++;
        if (is_running == false) {
          break;
        }
      }  // end while (ExecuteQuery(ret_query) == false)
    }
    /////////////////////////////////////////////////
    // Execute success: the memory should be deleted
    /////////////////////////////////////////////////
    else {  // execute == true
      if (ret_query != nullptr) {
        peloton::PlanNodeType plan_type = ret_query->GetPlanType();
        // Delete the query
        switch (plan_type) {
          case PLAN_NODE_TYPE_UPDATE: {
            reinterpret_cast<UpdateQuery *>(ret_query)->Cleanup();
            delete ret_query;
            break;
          }
          default: {
            LOG_ERROR("plan_type :: Unsupported Plan Tag: %u ", plan_type);
            elog(INFO, "Query: ");
            break;
          }
        }
      } else {  // queue is empty
        LOG_INFO("Queue is empty");
        continue;  // go to the while beginning
      }            // end else queue is empty
    }              // end else execute == true
    transaction_count_ref++;
  }  // end while
}

void TestRunBackend(oid_t thread_id) {
  PinToCore(thread_id);

  auto update_ratio = state.update_ratio;
  oid_t &execution_count_ref = abort_counts[thread_id];
  oid_t &transaction_count_ref = commit_counts[thread_id];
  ZipfDistribution zipf(state.scale_factor * 1000 - 1, state.zipf_theta);
  fast_random rng(rand());

  // backoff
  uint32_t backoff_shifts = 0;
  while (true) {
    if (is_running == false) {
      break;
    }

    auto rng_val = rng.next_uniform();

    /////////////////////////////////////////////////////////////////////////////
    // Use Queue_scheduler
    ////////////////////////////////////////////////////////////////////////////
    if (state.queue_scheduler > 0) {

      // GenerateAndQueueUpdate into queue
      if (rng_val < update_ratio) {
        // Generate # of queuries into the queue
        for (int i = 0; i < state.queue_scheduler; i++) {
          GenerateAndQueueUpdate(zipf);
        }
      } else {
        // Generate # of queries into the queue
        for (int i = 0; i < state.queue_scheduler; i++) {
          GenerateAndQueueUpdate(zipf);
        }
      }

      // Pop a query from a queue and execute
      // Execute a query
      concurrency::TransactionQuery *ret_query;

      //////////////////////////////////////////
      // Execute fail : retry
      //////////////////////////////////////////
      if (PopAndExecuteQuery(ret_query) == false) {
        execution_count_ref++;
        if (is_running == false) {
          break;
        }
        // backoff
        if (state.run_backoff) {
          if (backoff_shifts < 63) {
            ++backoff_shifts;
          }
          uint64_t spins = 1UL << backoff_shifts;
          spins *= 100;
          while (spins) {
            _mm_pause();
            --spins;
          }
        }  // if state.run_backoff

        // retry to execute the failed query
        while (ExecuteQuery(ret_query) == false) {
          execution_count_ref++;
          if (is_running == false) {
            break;
          }
          // backoff
          if (state.run_backoff) {
            if (backoff_shifts < 63) {
              ++backoff_shifts;
            }
            uint64_t spins = 1UL << backoff_shifts;
            spins *= 100;
            while (spins) {
              _mm_pause();
              --spins;
            }  // end while (spins)
          }    // end if (state.run_backoff)
        }      // end while (ExecuteQuery(ret_query) == false)
      }
      /////////////////////////////////////////////////
      // Execute success: the memory should be deleted
      /////////////////////////////////////////////////
      else {  // execute == true
        if (ret_query != nullptr) {
          peloton::PlanNodeType plan_type = ret_query->GetPlanType();
          // Delete the query
          switch (plan_type) {
            case PLAN_NODE_TYPE_UPDATE: {
              reinterpret_cast<UpdateQuery *>(ret_query)->Cleanup();
              delete ret_query;
              break;
            }
            default: {
              LOG_ERROR("plan_type :: Unsupported Plan Tag: %u ", plan_type);
              elog(INFO, "Query: ");
              break;
            }
          }
        } else {  // queue is empty
                  // TODO: if queue is empty
        }         // end else queue is empty
      }           // end else execute == true
    }
    ////////////////////////////////////////////////////////////////////////////
    // Queue_scheduler is not used, the thread directly execute the query
    ////////////////////////////////////////////////////////////////////////////
    else {
      UpdateQuery *query = nullptr;
      if (rng_val < update_ratio) {
        query = GenerateUpdate(zipf);
      } else {
        query = GenerateUpdate(zipf);
      }

      // Execute a query
      while (ExecuteQuery(query) == false) {
        execution_count_ref++;
        if (is_running == false) {
          break;
        }
        // backoff
        if (state.run_backoff) {
          if (backoff_shifts < 63) {
            ++backoff_shifts;
          }
          uint64_t spins = 1UL << backoff_shifts;
          spins *= 100;
          while (spins) {
            _mm_pause();
            --spins;
          }
        }  // if state.run_backoff
      }    // if execute == false
    }      // end else (state.queue_scheduler == false)
    transaction_count_ref++;
  }  // end while(true)
}

void TestRunWorkload() {
  // Execute the workload to build the log
  std::vector<std::thread> thread_group;
  oid_t num_threads = state.backend_count;
  abort_counts = new oid_t[num_threads];
  memset(abort_counts, 0, sizeof(oid_t) * num_threads);
  commit_counts = new oid_t[num_threads];
  memset(commit_counts, 0, sizeof(oid_t) * num_threads);
  generate_counts = new oid_t[state.queue_scheduler];
  memset(generate_counts, 0, sizeof(oid_t) * state.queue_scheduler);

  size_t snapshot_round = (size_t)(state.duration / state.snapshot_duration);
  oid_t **abort_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    abort_counts_snapshots[round_id] = new oid_t[num_threads];
  }
  oid_t **commit_counts_snapshots = new oid_t *[snapshot_round];
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    commit_counts_snapshots[round_id] = new oid_t[num_threads];
  }

  // Launch a bunch of threads to execute the query // ExecuteBackend
  for (oid_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(PopQuery, thread_itr)));
  }

  // Launch a bunch of threads to queue the query
  for (oid_t thread_itr = num_threads;
       thread_itr < num_threads + state.queue_scheduler; ++thread_itr) {
    thread_group.push_back(std::move(std::thread(QueryBackend, thread_itr)));
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    std::this_thread::sleep_for(
        std::chrono::milliseconds(int(state.snapshot_duration * 1000)));
    memcpy(abort_counts_snapshots[round_id], abort_counts,
           sizeof(oid_t) * num_threads);
    memcpy(commit_counts_snapshots[round_id], commit_counts,
           sizeof(oid_t) * num_threads);
  }
  is_running = false;

  // Join the threads with the main thread
  for (oid_t thread_itr = 0; thread_itr < num_threads + state.queue_scheduler;
       ++thread_itr) {
    thread_group[thread_itr].join();
  }

  // calculate the generate rate
  oid_t total_generate_count = 0;
  for (int i = 0; i < state.queue_scheduler; ++i) {
    total_generate_count += generate_counts[i];
  }
  state.generate_rate = total_generate_count * 1.0 / state.duration;

  // calculate the throughput and abort rate for the first round.
  oid_t total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[0][i];
  }

  oid_t total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[0][i];
  }

  state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                      state.snapshot_duration);
  state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                      total_commit_count);

  // calculate the throughput and abort rate for the remaining rounds.
  for (size_t round_id = 0; round_id < snapshot_round - 1; ++round_id) {
    total_commit_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_commit_count += commit_counts_snapshots[round_id + 1][i] -
                            commit_counts_snapshots[round_id][i];
    }

    total_abort_count = 0;
    for (size_t i = 0; i < num_threads; ++i) {
      total_abort_count += abort_counts_snapshots[round_id + 1][i] -
                           abort_counts_snapshots[round_id][i];
    }

    state.snapshot_throughput.push_back(total_commit_count * 1.0 /
                                        state.snapshot_duration);
    state.snapshot_abort_rate.push_back(total_abort_count * 1.0 /
                                        total_commit_count);
  }

  // calculate the aggregated throughput and abort rate.
  total_commit_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_commit_count += commit_counts_snapshots[snapshot_round - 1][i];
  }

  total_abort_count = 0;
  for (size_t i = 0; i < num_threads; ++i) {
    total_abort_count += abort_counts_snapshots[snapshot_round - 1][i];
  }

  state.throughput = total_commit_count * 1.0 / state.duration;
  state.abort_rate = total_abort_count * 1.0 / total_commit_count;

  // cleanup everything.
  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] abort_counts_snapshots[round_id];
    abort_counts_snapshots[round_id] = nullptr;
  }

  for (size_t round_id = 0; round_id < snapshot_round; ++round_id) {
    delete[] commit_counts_snapshots[round_id];
    commit_counts_snapshots[round_id] = nullptr;
  }
  delete[] abort_counts_snapshots;
  abort_counts_snapshots = nullptr;
  delete[] commit_counts_snapshots;
  commit_counts_snapshots = nullptr;

  delete[] abort_counts;
  abort_counts = nullptr;
  delete[] commit_counts;
  commit_counts = nullptr;
}
//-b 1 -u 1 -z 0.9 -p occ -g co -d 10 -q
// calculate the total_throughput_simple_count
// oid_t total_throughput_simple_count = 0;
// for (oid_t i = 0; i < num_threads; ++i) {
//  total_throughput_simple_count += commit_counts[i];
//}
// state.throughput_simple =
//    total_throughput_simple_count * 1.0 / state.duration;
//
//// calculate the abort_rate_simple
// oid_t abort_simple = 0;
// for (oid_t i = 0; i < num_threads; ++i) {
//  abort_simple += abort_counts[i];
//}
// state.abort_rate_simple = abort_simple * 1.0 / total_throughput_simple_count;
}  // namespace ycsb
}  // namespace benchmark
}  // namespace peloton
