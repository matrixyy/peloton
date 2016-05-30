//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_manager.h
//
// Identification: src/backend/concurrency/transaction_manager.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/lockfree_queue.h"
#include "backend/planner/abstract_plan.h"
#include "backend/executor/abstract_executor.h"

namespace peloton {
namespace concurrency {

////////////////////////////////
// Transaction Scheduler is responsible for
// 1) queuing query plan. txn-schd takes plan-tree/plan-node as a parameter.
//      Besides, param_list and tuple_desc is also needed as parameters.
//      1 - Txn-schd puts these three parameters into a queue or several queues.
//      2 - Txn-schd compare the recived plan-tree with the current plans in the
// queue
//          (If the plan-tree is a update, the comparision is reseanable)
//          If the updated data by the received plan has overlap with some other
// txns,
//          the received plan should be put into that queue.
// 2) execute query plan. pick up a plan-tree from a queue and execute it.
//      1 - pick up a plan from a give queue
//      2 - pick up a plan (round robin)
///////////////////////////////

class TransactionQuery {
 public:
  //  TransactionQuery(executor::AbstractExecutor* executor) {
  //    executor_tree_ = executor;
  //    b_executor_ = true;
  //    type_ = PlanNodeType::PLAN_NODE_TYPE_INVALID;
  //  }
  //
  //  TransactionQuery(executor::AbstractExecutor* executor, PlanNodeType type)
  // {
  //    executor_tree_ = executor;
  //    b_executor_ = true;
  //    type_ = type;
  //  }
  TransactionQuery() {}
  virtual ~TransactionQuery() {}
  // executor::AbstractExecutor* GetExecutor() { return executor_tree_; }
  // PlanNodeType GetPlanType() { return type_; }
  virtual PlanNodeType GetPlanType() = 0;

  // private:
  // Plan_tree is a pointer shared with the passing by object
  // std::shared_ptr<planner::AbstractPlan> plan_tree_;

  // param_list is a pointer shared with the passing by object
  // std::shared_ptr<std::vector<peloton::Value>> param_list_;

  // Tuple_desc is a pointer shared with the passing by object
  // std::shared_ptr<TupleDesc> tuple_desc_;

  // Executor tree. A query can be a executor tree. Then it doest need other
  // params to execute
  // std::shared_ptr<const peloton::executor::AbstractExecutor> executor_tree_;
  // executor::AbstractExecutor* executor_tree_;

  // A flag to indicates this query is a plan or executor
  // bool b_executor_;

  // Node type
  // PlanNodeType type_;
};

class TransactionScheduler {
 public:
  // Singleton
  static TransactionScheduler& GetInstance();

  TransactionScheduler() { count_queue_ = 0; }
  TransactionScheduler(int count_queue) { count_queue_ = count_queue; }

  // Enqueue the query into the concurrency_queue
  void SimpleEnqueue(TransactionQuery* query) {
    concurrency_queue_.Enqueue(query);
  }

  // Pop from the concurrency_queue. Note: query should be reference type
  bool SimpleDequeue(TransactionQuery*& query) {
    return concurrency_queue_.Dequeue(query);
  }
  void Enqueue(TransactionQuery* query);
  bool Dequeue(TransactionQuery*& query);
  bool Dequeue(TransactionQuery*& query, int num_queue);

 private:
  int count_queue_;

  // FIXME: I'd like to use unique_ptr, but not sure LockFreeQueue supports
  // smart pointer
  std::vector<LockfreeQueue<TransactionQuery*>> conflict_queues_;
  LockfreeQueue<TransactionQuery*> concurrency_queue_;
};

}  // end namespace concurrency
}  // end namespace peloton
