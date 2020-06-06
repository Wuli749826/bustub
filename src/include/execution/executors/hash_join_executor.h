//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <queue>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "container/hash/linear_probe_hash_table.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {
/**
 * IdentityHashFunction hashes everything to itself, i.e. h(x) = x.
 */
class IdentityHashFunction : public HashFunction<hash_t> {
 public:
  /**
   * Hashes the key.
   * @param key the key to be hashed
   * @return the hashed value
   */
  uint64_t GetHash(size_t key) override { return key; }
};

/**
 * A simple hash table that supports hash joins.
 */
class SimpleHashJoinHashTable {
 public:
  /** Creates a new simple hash join hash table. */
  SimpleHashJoinHashTable(const std::string &name, BufferPoolManager *bpm, HashComparator cmp, uint32_t buckets,
                          const IdentityHashFunction &hash_fn) {}

  /**
   * Inserts a (hash key, tuple) pair into the hash table.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param t the tuple to associate with the key
   * @return true if the insert succeeded
   */
  bool Insert(Transaction *txn, hash_t h, const Tuple &t) {
    hash_table_[h].emplace_back(t);
    return true;
  }

  /**
   * Gets the values in the hash table that match the given hash key.
   * @param txn the transaction that we execute in
   * @param h the hash key
   * @param[out] t the list of tuples that matched the key
   */
  void GetValue(Transaction *txn, hash_t h, std::vector<Tuple> *t) { *t = hash_table_[h]; }

 private:
  std::unordered_map<hash_t, std::vector<Tuple>> hash_table_;
};

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
using HT = SimpleHashJoinHashTable;

// using HashJoinKeyType = ???;
// using HashJoinValType = ???;
// using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, HashComparator>;

/**
 * HashJoinExecutor executes hash join operations.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new hash join executor.
   * @param exec_ctx the context that the hash join should be performed in
   * @param plan the hash join plan node
   * @param left the left child, used by convention to build the hash table
   * @param right the right child, used by convention to probe the hash table
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan, std::unique_ptr<AbstractExecutor> &&left,
                   std::unique_ptr<AbstractExecutor> &&right)
      : AbstractExecutor(exec_ctx) {
        plan_ = plan;
        // SimpleCatalog* catalog_ = exec_ctx_->GetCatalog();
        // metadata_ = catalog_->GetTable(table_oid_);
        // tableheap_ = metadata_->table_.get();
        predicate_ = plan_->Predicate();
        left_plan_ = plan->GetLeftPlan();
        right_plan_ = plan_->GetRightPlan();
        left_executor_ = std::move(left);
        right_executor_ = std::move(right);
      }

  /** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
  // Uncomment me! const HT *GetJHT() const { return &jht_; }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }


  void Init() override {
      jhtable_ = new HT ("jht", exec_ctx_->GetBufferPoolManager() , jht_comp_, jht_num_buckets_, jht_hash_fn_);
      left_executor_->Init();
      Tuple t;
      while (left_executor_->Next(&t)) {
          jhtable_->Insert(exec_ctx_->GetTransaction(), HashValues(&t, left_plan_->OutputSchema(), plan_->GetLeftKeys()), t);
      }
      right_executor_->Init();
  }

  // probe the right child
  //notify : the left child may match a lot for one right child
  bool Next(Tuple *tuple) override {
    if (leave.size() > 0) {
        *tuple = leave.front();
        leave.pop();
        return true;
    }
    Tuple t;
    if (right_executor_->Next(&t) == false)
        return false;
    hash_t num_bucket_ = HashValues(&t, right_plan_->OutputSchema(), plan_->GetRightKeys());
    //how to define the size of vector
    std::vector<Tuple> set{1000};
    jhtable_->GetValue(exec_ctx_->GetTransaction(), num_bucket_, &set);
    for (auto i : set) {
        if (predicate_ == nullptr || predicate_->EvaluateJoin(&t, right_plan_->OutputSchema(), &i, left_plan_->OutputSchema()).GetAs<bool> ())
            leave.push(t);
    }
    if (leave.size() == 0)
        return false;
    *tuple = leave.front();
    leave.pop();
    return true;
  }

  /**
   * Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
   * @param tuple tuple to be hashed
   * @param schema schema to evaluate the tuple on
   * @param exprs expressions to evaluate the tuple with
   * @return the hashed tuple
   */
  hash_t HashValues(const Tuple *tuple, const Schema *schema, const std::vector<const AbstractExpression *> &exprs) {
    hash_t curr_hash = 0;
    // For every expression,
    for (const auto &expr : exprs) {
      // We evaluate the tuple on the expression and schema.
      Value val = expr->Evaluate(tuple, schema);
      // If this produces a value,
      if (!val.IsNull()) {
        // We combine the hash of that value into our current hash.
        curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val));
      }
    }
    return curr_hash;
  }

 private:
  /** The hash join plan node. */
  const HashJoinPlanNode* plan_;
  /** The comparator is used to compare hashes. */
  [[maybe_unused]] HashComparator jht_comp_{};
  /** The identity hash function. */
  IdentityHashFunction jht_hash_fn_{};

  /** The hash table that we are using. */
  // Uncomment me! HT jht_;
  /** The number of buckets in the hash table. */
  static constexpr uint32_t jht_num_buckets_ = 2;

  //add by myself
  const AbstractPlanNode * left_plan_;
  const AbstractPlanNode * right_plan_;
  const AbstractExpression *predicate_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::queue<Tuple> leave;

  HT* jhtable_;
};
}
// namespace bustub
