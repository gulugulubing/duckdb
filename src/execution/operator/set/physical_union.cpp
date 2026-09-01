#include "duckdb/execution/operator/set/physical_union.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalUnion::PhysicalUnion(PhysicalPlan &physical_plan, vector<LogicalType> types_p,
                             const ArenaLinkedList<reference<PhysicalOperator>> &children_p,
                             idx_t estimated_cardinality, bool allow_out_of_order,
                             vector<pair<idx_t, idx_t>> sink_work_dependencies_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::UNION, std::move(types_p), estimated_cardinality),
      allow_out_of_order(allow_out_of_order), sink_work_dependencies(std::move(sink_work_dependencies_p)) {
	for (auto &child : children_p) {
		children.push_back(child);
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
static bool ContainsSink(PhysicalOperator &op) {
	if (op.IsSink()) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsSink(child)) {
			return true;
		}
	}
	return false;
}

void PhysicalUnion::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	// order matters if any of the downstream operators are order dependent,
	// or if the sink preserves order, but does not support batch indices to do so
	auto sink = meta_pipeline.GetSink();
	bool order_matters = false;
	if (!allow_out_of_order) {
		order_matters = true;
	}
	if (current.IsOrderDependent()) {
		order_matters = true;
	}
	if (sink) {
		if (Settings::Get<PreserveInsertionOrderSetting>(current.GetClientContext()) && sink->SinkOrderDependent()) {
			order_matters = true;
		}
		auto partition_info = sink->RequiredPartitionInfo();
		if (partition_info.batch_index) {
			order_matters = true;
		}
		if (!sink->ParallelSink()) {
			order_matters = true;
		}
	}

	// create union pipelines that has identical dependencies to 'current'
	vector<reference<Pipeline>> union_pipelines;
	for (idx_t i = 0; i + 1 < children.size(); i++) {
		auto &union_pipeline = meta_pipeline.CreateUnionPipeline(current, order_matters);
		union_pipelines.push_back(union_pipeline);
	}
	// collect the sink work (the child meta pipelines) created by building each child
	auto collect_sink_work = [&meta_pipeline](idx_t start) {
		vector<shared_ptr<Pipeline>> result;
		auto &meta_children = meta_pipeline.GetChildren();
		for (idx_t k = start; k < meta_children.size(); k++) {
			result.push_back(meta_children[k]->GetBasePipeline());
		}
		return result;
	};
	// continue with the current pipeline
	idx_t child_start = meta_pipeline.GetChildren().size();
	children[0].get().BuildPipelines(current, meta_pipeline);
	vector<vector<shared_ptr<Pipeline>>> sink_work_bases(children.size());
	sink_work_bases[0] = collect_sink_work(child_start);
	bool can_saturate_threads =
	    ContainsSink(children[0].get()) && children[0].get().CanSaturateThreads(current.GetClientContext());
	for (idx_t i = 1; i < children.size(); i++) {
		auto &union_pipeline = union_pipelines[children.size() - i - 1].get();
		vector<shared_ptr<Pipeline>> dependencies;
		optional_ptr<MetaPipeline> last_child_ptr;
		if (ContainsSink(children[i - 1].get()) &&
		    children[i - 1].get().CanSaturateThreads(current.GetClientContext())) {
			can_saturate_threads = true;
		}
		if (order_matters || can_saturate_threads) {
			// we add dependencies if order matters: union_pipeline comes after all pipelines created by building
			// current
			auto dependency_type =
			    order_matters ? MetaPipelineDependencyType::REQUIRED : MetaPipelineDependencyType::OPTIONAL_DEPENDENCY;
			dependencies = meta_pipeline.AddDependenciesFrom(union_pipeline, union_pipeline, false, dependency_type);
			// we also add dependencies if the LHS child can saturate all available threads
			// in that case, we recursively make all RHS children depend on the LHS.
			// This prevents breadth-first plan evaluation
			if (can_saturate_threads) {
				last_child_ptr = meta_pipeline.GetLastChild();
			}
		}
		// Assign proper batch index to the union pipeline
		meta_pipeline.AssignNextBatchIndex(union_pipeline);
		// build the union pipeline
		child_start = meta_pipeline.GetChildren().size();
		children[i].get().BuildPipelines(union_pipeline, meta_pipeline);
		sink_work_bases[i] = collect_sink_work(child_start);

		// The sink work of this child must only start after the sink work of the children it depends
		// on has fully completed (including its finalization) - e.g. when a child inserts data that is
		// checked against data inserted by a previous child (foreign key constraints).
		// We depend on the base pipeline of the dependency's sink work: its completion covers all
		// pipelines of that meta pipeline and its finalization.
		if (!sink_work_dependencies.empty()) {
			for (auto &dep : sink_work_dependencies) {
				if (dep.second != i) {
					continue;
				}
				auto &dep_bases = sink_work_bases[dep.first];
				if (dep_bases.empty()) {
					continue;
				}
				for (idx_t k = child_start; k < meta_pipeline.GetChildren().size(); k++) {
					vector<shared_ptr<Pipeline>> pipelines;
					meta_pipeline.GetChildren()[k]->GetPipelines(pipelines, false);
					for (auto &pipeline : pipelines) {
						for (auto &dep_base : dep_bases) {
							pipeline->AddDependency(dep_base);
						}
					}
				}
			}
		}

		if (last_child_ptr) {
			// the pointer was set, set up the dependencies
			meta_pipeline.AddRecursiveDependencies(dependencies, *last_child_ptr);
		}
	}
}

vector<const_reference<PhysicalOperator>> PhysicalUnion::GetSources() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		auto child_sources = child.get().GetSources();
		result.insert(result.end(), child_sources.begin(), child_sources.end());
	}
	return result;
}

} // namespace duckdb
