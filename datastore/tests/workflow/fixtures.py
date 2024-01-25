import pytest


@pytest.fixture
def subset_query() -> str:
    yield """
    {
        "dataset_id": "era5-single-levels",
        "product_id": "reanalysis",
        "query": {
            "area": {
                "north": -85,
                "south": -90,
                "east": 260,
                "west": 240
            },
            "time": {
                "hour": [
                    "15"
                ],
                "year": [
                    "1981",
                    "1985",
                    "2022"
                ],
                "month": [
                    "3",
                    "6"
                ],
                "day": [
                    "23",
                    "27"
                ]
            },
            "variable": [
                "2_metre_dewpoint_temperature",
                "surface_net_downward_shortwave_flux"
            ]            
        }
    }
    """


@pytest.fixture
def resample_query():
    yield """
    {
        "freq": "1D",
        "operator": "nanmax",
        "resample_args": {
            "closed": "right"
        }
    }
    """


@pytest.fixture
def workflow_str():
    yield """
    [
        {
            "id": "subset1",
            "op": "subset",
            "args": {
                "dataset_id": "era5-single-levels",
                "product_id": "reanalysis",
                "query": {
                    "area": {
                        "north": -85,
                        "south": -90,
                        "east": 260,
                        "west": 240
                        }              
                    }
                }
        },
        {
            "id": "resample1",
            "use": ["subset1"],
            "op": "resample",
            "args": 
            {
                "freq": "1D",
                "operator": "nanmax"
            }
        }
    ]
    """


@pytest.fixture
def bad_workflow_str():
    yield """
    [
        {
            "id": "subset1",
            "op": "subset",
            "args": {
                "dataset_id": "era5-single-levels",
                "product_id": "reanalysis",
                "query": {
                    "area": {
                        "north": -85,
                        "south": -90,
                        "east": 260,
                        "west": 240
                        }              
                    }
                }
        },
        {
            "id": "resample1",
            "use": ["subset1", "subset2"],
            "op": "resample",
            "args": 
            {
                "freq": "1D",
                "operator": "nanmax"
            }
        }
    ]
    """
