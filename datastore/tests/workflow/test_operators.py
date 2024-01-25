from workflow import operators as op

from .fixtures import subset_query, resample_query


def test_create_subset_operator_with_str_args(subset_query):
    sub_op = op.Operator("subset", subset_query)
    assert isinstance(sub_op, op.Subset)
    assert isinstance(sub_op.args, op.SubsetArgs)
    assert sub_op.args.dataset_id == "era5-single-levels"
    assert sub_op.args.product_id == "reanalysis"


def test_create_resample_operator_with_str_args(resample_query):
    res_op = op.Operator("resample", resample_query)
    assert isinstance(res_op, op.Resample)
    assert isinstance(res_op.args, op.ResampleArgs)
    assert res_op.args.freq == "1D"
    assert res_op.args.operator == "nanmax"
    assert res_op.args.resample_args == {"closed": "right"}
