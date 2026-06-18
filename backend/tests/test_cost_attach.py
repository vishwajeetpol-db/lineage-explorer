"""
Serve-time cost attachment. Cost must be re-attached from the live cost cache
when a response is served, NOT baked into the cached graph — otherwise a
cold-start race (cost cache not yet populated when the graph was built) hides
cost for the full cache TTL. Also verifies the cached object isn't mutated.
"""
from backend import lineage_service
from backend.models import EntityNode, LineageResponse


def _resp():
    return LineageResponse(
        nodes=[
            EntityNode(id="entity:PIPELINE:pid-1", entity_type="PIPELINE", entity_id="pid-1"),
            EntityNode(id="entity:JOB:job-1", entity_type="JOB", entity_id="job-1"),
        ],
        edges=[],
    )


class TestServeTimeCostAttach:
    def test_cost_attached_from_live_cache(self, monkeypatch):
        monkeypatch.setattr(lineage_service, "_cost_by_pipeline_id", {"pid-1": 1.23})
        monkeypatch.setattr(lineage_service, "_cost_by_job_id", {"job-1": 4.5})
        original = _resp()
        wrapped = lineage_service._wrap_with_cache_metadata(original, "lineage:x", from_cache=True)
        by_id = {n.entity_id: n.cost_usd for n in wrapped.nodes}
        assert by_id == {"pid-1": 1.23, "job-1": 4.5}
        # cached object must remain untouched (immutability across requests)
        assert all(n.cost_usd is None for n in original.nodes)

    def test_empty_cost_cache_leaves_none(self, monkeypatch):
        monkeypatch.setattr(lineage_service, "_cost_by_pipeline_id", {})
        monkeypatch.setattr(lineage_service, "_cost_by_job_id", {})
        wrapped = lineage_service._wrap_with_cache_metadata(_resp(), "lineage:x", from_cache=True)
        assert all(n.cost_usd is None for n in wrapped.nodes)

    def test_stale_cached_cost_is_refreshed(self, monkeypatch):
        # a node cached with an old cost gets the current value at serve time
        monkeypatch.setattr(lineage_service, "_cost_by_pipeline_id", {"pid-1": 9.99})
        monkeypatch.setattr(lineage_service, "_cost_by_job_id", {})
        stale = LineageResponse(
            nodes=[EntityNode(id="entity:PIPELINE:pid-1", entity_type="PIPELINE",
                              entity_id="pid-1", cost_usd=0.01)],
            edges=[],
        )
        wrapped = lineage_service._wrap_with_cache_metadata(stale, "lineage:x", from_cache=True)
        assert wrapped.nodes[0].cost_usd == 9.99
