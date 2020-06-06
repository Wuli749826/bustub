//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"
#include "storage/table/table_iterator.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */

//   SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
//       //in the baseclass constructor, exec_ctx will be initialized
//       plan_ = plan;
//   }
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
    plan_ = plan;
    table_oid_t table_oid_ = plan_->GetTableOid();
    SimpleCatalog* catalog_ = exec_ctx_->GetCatalog();
    metadata_ = catalog_->GetTable(table_oid_);
    tableheap_ = metadata_->table_.get();

  } 

  void Init() override {
    abs_expression_ = plan_->GetPredicate();
    // if the table is empty, no tuple
    if (tableheap_->Begin(exec_ctx_->GetTransaction()) == tableheap_->End()) {
        begin_ = new TableIterator(tableheap_, RID(INVALID_PAGE_ID, 0), exec_ctx_->GetTransaction());
        return;
    }
    Tuple tmp = *(tableheap_->Begin(exec_ctx_->GetTransaction()));
    begin_ = new TableIterator(tableheap_, tmp.GetRid(), exec_ctx_->GetTransaction());
  }

  bool Next(Tuple *tuple) override { 
    if (*begin_ == tableheap_->End())
        return false;
    *tuple = *(*begin_);
    // check whether this tuple satisfy the predicate
    //check whether there is predicate

    if (abs_expression_ == nullptr || abs_expression_->Evaluate(tuple, &(metadata_->schema_)).GetAs<bool> ()) {
        ++(*begin_);
        return true;
    }
    ++(*begin_);
    return Next(tuple); 
}

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;

  //add by myself
  TableMetadata * metadata_;
  TableHeap* tableheap_;
  TableIterator* begin_;
  const AbstractExpression * abs_expression_;


};
}  // namespace bustub
