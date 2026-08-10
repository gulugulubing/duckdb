//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/rule/comparison_simplification.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"

namespace duckdb {

// The Comparison Simplification rule rewrites comparisons with a constant NULL (i.e. [x = NULL] => [NULL])
class ComparisonSimplificationRule : public Rule {
public:
	explicit ComparisonSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

//! Rewrites top-level filter equality between row constructors into scalar equalities.
class RowComparisonSimplificationRule : public Rule {
public:
	explicit RowComparisonSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

//! Rewrites top-level filter equality between a row constructor and a folded row constant
//! (e.g. (id, b) = (42, 2)) into scalar equalities.
class ConstantRowComparisonSimplificationRule : public Rule {
public:
	explicit ConstantRowComparisonSimplificationRule(ExpressionRewriter &rewriter);

	unique_ptr<Expression> Apply(LogicalOperator &op, vector<reference<Expression>> &bindings, bool &changes_made,
	                             bool is_root) override;
};

} // namespace duckdb
