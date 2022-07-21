__version__ = "2.0"

from typing import Optional
from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import FileResponse
from enum import Enum
from pydantic import BaseModel

app = FastAPI(
    title="geokube-dds API for Webportal",
    description="REST API for DDS Webportal",
    version=__version__,
    contact={
        "name": "geokube Contributors",
        "email": "geokube@googlegroups.com",
    },
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
)


@app.get("/")
async def dds_info():
    """Return current version of the DDS API for the Webportal

    Returns
    -------
    version : str
        A string including version of DDS

    """
    return f"DDS Webportal API {__version__}"
