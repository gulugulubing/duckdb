//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/set/physical_union.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalUnion : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UNION;

public:
	PhysicalUnion(PhysicalPlan &physical_plan, vector<LogicalType> types,
	              const ArenaLinkedList<reference<PhysicalOperator>> &children_p, idx_t estimated_cardinality,
	              bool allow_out_of_order, vector<pair<idx_t, idx_t>> sink_work_dependencies = {});

	bool allow_out_of_order;
	//! Pairs of child indices (dependency source, dependant): the sink work of the dependant child
	//! (e.g. its INSERT) may only start after the sink work of the dependency source has fully completed.
	//! Used by COPY FROM DATABASE to serialize foreign-key dependent inserts.
	vector<pair<idx_t, idx_t>> sink_work_dependencies;

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	vector<const_reference<PhysicalOperator>> GetSources() const override;
};

} // namespace duckdb
