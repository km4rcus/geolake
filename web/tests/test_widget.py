import pytest
import json

data = """
{
    "metadata": {
        "catalog_dir": "/catalog/",
        "description": "BioClim",
        "label": "Bioclimatic Indicators",
        "how_to_cite": "Wheneve",
        "contact": {
            "name": "Sergio Noce",
            "email": "sergio.noce@cmcc.it",
            "webpage": "https://www.cmcc.it/people/noce-sergio"
        },
        "image": "https://ddsfiles.s3.fr-par.scw.cloud/images/bioclimind.jpg",
        "doi": "https://doi.org/10.25424/CMCC/BIOCLIMIND",
        "license": {
            "name": "Creative Commons Attribution 4.0 International (CC BY 4.0)",
            "url": "https://creativecommons.org/licenses/by/4.0"
        },
        "publication_date": "2020-12-22",
        "keywords": [
            "Bioclimatic indicators",
            "Ecological modeling",
            "CMIP5",
            "Biogeography",
            "Species Distribution Modeling"
        ],
        "related_data": [
            {
                "name": "A new global dataset of bioclimatic indicators",
                "url": "https://doi.org/10.1038/s41597-020-00726-5"
            }
        ],
        "id": "bioclimind"
    },
    "products": {
        "future": {
            "role": "internal",
            "filters": [
                {
                    "name": "rcp",
                    "user_defined": "T"
                },
                {
                    "name": "time_interval",
                    "user_defined": "T"
                },
                {
                    "name": "CMIP5",
                    "user_defined": "T"
                },
                {
                    "name": "var"
                }
            ],
            "catalog_dir": "/catalog/cmcc/",
            "details": [
                {
                    "datacube": {
                        "domain": {
                            "crs": {
                                "name": "latitude_longitude",
                                "semi_major_axis": 6371229.0,
                                "semi_minor_axis": 6371229.0,
                                "inverse_flattening": 0.0,
                                "longitude_of_prime_meridian": 0.0
                            },
                            "coordinates": {
                                "latitude": {
                                    "values": [
                                        89.75,
                                        89.25,
                                        -89.25,
                                        -89.75
                                    ],
                                    "units": "degrees_north",
                                    "axis": "LATITUDE"
                                },
                                "longitude": {
                                    "values": [
                                        -179.75,
                                        -179.2,
                                        179.25,
                                        179.75
                                    ],
                                    "units": "degrees_east",
                                    "axis": "LONGITUDE"
                                }
                            }
                        },
                        "fields": {
                            "time_bnds": {
                                "units": "unknown"
                            },
                            "BIO1": {
                                "units": "Celsius"
                            }
                        }
                    },
                    "attributes": {
                        "CMIP5": "CMCC",
                        "rcp": "85",
                        "time_interval": "2040_79",
                        "var": "BIO1"
                    }
                },
                {
                    "datacube": {
                        "domain": {
                            "crs": {
                                "name": "latitude_longitude",
                                "semi_major_axis": 6371229.0,
                                "semi_minor_axis": 6371229.0,
                                "inverse_flattening": 0.0,
                                "longitude_of_prime_meridian": 0.0
                            },
                            "coordinates": {
                                "latitude": {
                                    "values": [
                                        89.75,
                                        89.25,
                                        -89.75
                                    ],
                                    "units": "degrees_north",
                                    "axis": "LATITUDE"
                                },
                                "longitude": {
                                    "values": [
                                        -179.75,
                                        179.25,
                                        179.75
                                    ],
                                    "units": "degrees_east",
                                    "axis": "LONGITUDE"
                                }
                            }
                        },
                        "fields": {
                            "bio10": {
                                "units": "Celsius"
                            }
                        }
                    },
                    "attributes": {
                        "CMIP5": "CMCC",
                        "rcp": "85",
                        "time_interval": "2040_79",
                        "var": "BIO10"
                    }
                }
            ]
        }
    }
}
"""
dict = json.loads(data)

from app.widget import _Dataset, WidgetFactory

ds = _Dataset(**dict)
wf = WidgetFactory(ds)
breakpoint()
wf.get_widgets_for("future")
pass
