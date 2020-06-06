//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new insert executor.
   * @param exec_ctx the executor context
   * @param plan the insert plan to be executed
   * @param child_executor the child executor to obtain insert values from, can be nullptr
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor)
      : AbstractExecutor(exec_ctx), plan_(plan) {
          table_oid_t table_oid_ = plan_->TableOid();
        SimpleCatalog* catalog_ = exec_ctx_->GetCatalog();
        metadata_ = catalog_->GetTable(table_oid_);
        tableheap_ = metadata_->table_.get();
      }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

  void Init() override {
    //we need to insert the raw values in the plannode, whether its rawinsert or not
    if (plan_->IsRawInsert() == false)
        return;
    if (plan_->RawValues().size() == 0)
        return;
    for (auto i : plan_->RawValues()) {
        Schema schema_ = metadata_->schema_;
        Tuple t(i, &schema_);
        RID t_rid_ = t.GetRid();
        tableheap_->InsertTuple(t, &t_rid_, exec_ctx_->GetTransaction());
    }
  }

  // Note that Insert does not make use of the tuple pointer being passed in.
  // We return false if the insert failed for any reason, and return true if all inserts succeeded.
  bool Next([[maybe_unused]] Tuple *tuple) override {
    if (plan_->IsRawInsert()) {
        // inti has done the work, insert all tuples from rawvalues
        return true;
    }
    // use the child node to achieve next
    const AbstractPlanNode* child_plan_ =  plan_->GetChildPlan();
    auto executor = ExecutorFactory::CreateExecutor(exec_ctx_, child_plan_);
    executor->Init();
    Tuple t;
    while (executor->Next(&t)) {
        RID t_rid_ = t.GetRid();
        tableheap_->InsertTuple(t, &t_rid_, exec_ctx_->GetTransaction());
    }
    return true;
  }

 private:
  /** The insert plan node to be executed. */
  const InsertPlanNode *plan_;

  //add by myself
  TableMetadata * metadata_;
  TableHeap* tableheap_;
};
}  // namespace bustub
