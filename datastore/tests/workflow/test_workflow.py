import pytest
from workflow.workflow import Workflow

from .fixtures import workflow_str, bad_workflow_str


def test_create_workflow(workflow_str):
    comp_graph = Workflow(workflow_str)
    assert len(comp_graph) == 2
    task_iter = comp_graph.traverse()
    node1, precedint1 = next(task_iter)
    assert precedint1 == tuple()
    assert node1.operator.name == "subset"

    node2, precedint2 = next(task_iter)
    assert len(precedint2) == 1
    assert node2.operator.name == "resample"
    assert precedint2[0].operator.name == "subset"


def test_fail_when_task_not_defined(bad_workflow_str):
    with pytest.raises(ValueError, match=r"task with id*"):
        _ = Workflow(bad_workflow_str)
