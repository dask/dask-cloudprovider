from pydantic import BaseModel
from typing import Union


class FlyAppCreateRequest(BaseModel):
    app_name: str
    org_slug: str


class FlyAppDetailsResponse(BaseModel):
    name: str
    status: str
    organization: dict


class FlyAppDeleteRequest(BaseModel):
    app_name: str
    org_slug: Union[str, None] = None
    force: bool = False


class FlyAppDeleteResponse(BaseModel):
    app_name: str
    org_slug: str
    status: str
    organization: dict
