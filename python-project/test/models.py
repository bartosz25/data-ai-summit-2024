import dataclasses
import datetime
import json
from typing import Optional, Dict, Callable, Any, Set, List

from pyspark import Row


class WithRowConverter:

    def as_row(self, overrides: Optional[Dict[str, Callable]] = None,
               extra_fields: Optional[Dict[str, Any]] = None, ignored_fields: Optional[Set[str]] = None) -> Row:
        overrides = {} if overrides is None else overrides
        extra_fields = {} if extra_fields is None else extra_fields
        ignored_fields = set() if ignored_fields is None else ignored_fields
        row_fields = {}
        for field, value in self.__dict__.items():
            if field not in ignored_fields:
                if isinstance(value, list):
                    # TODO: refactor me, it's a quick fix for the tests only, probably better to use something else that the items() (use a param)
                    result = []
                    nested_rows = iter(value)
                    for nested_row in nested_rows:
                        result.append(nested_row.as_row())
                    row_fields[field] = result
                elif isinstance(value, WithRowConverter):
                    print(f'converting {value}')
                    row_fields[field] = value.as_row()
                else:
                    value_to_set = value
                    if field in overrides:
                        value_to_set = overrides[field](value)
                    row_fields[field] = value_to_set
        row_fields.update(extra_fields)
        return Row(**row_fields)


@dataclasses.dataclass
class TechnicalContext(WithRowConverter):
    browser: str
    browser_version: str
    network_type: str
    device_type: str
    device_version: str


@dataclasses.dataclass
class UserContext(WithRowConverter):
    ip: str
    login: Optional[str]
    connected_since: Optional[datetime.datetime]


@dataclasses.dataclass
class VisitContext(WithRowConverter):
    referral: str
    ad_id: Optional[str]
    user: UserContext
    technical: TechnicalContext


@dataclasses.dataclass
class Visit(WithRowConverter):
    visit_id: str
    event_time: datetime.datetime
    user_id: str
    page: str
    context: VisitContext

    def as_json(self) -> str:
        def encoder_datetime(obj):
            if isinstance(obj, datetime.datetime):
                return obj.isoformat(sep='T')

        return json.dumps(dataclasses.asdict(self), default=encoder_datetime)

    def as_kafka_row(self) -> Row:
        return Row(value=self.as_json())


@dataclasses.dataclass
class Device(WithRowConverter):
    type: str
    version: str
    full_name: str


@dataclasses.dataclass
class SessionPage(WithRowConverter):
    page: str
    visit_time: datetime.datetime


@dataclasses.dataclass
class Session(WithRowConverter):
    visit_id: str
    user_id: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    visited_pages: List[SessionPage]
    device_type: Optional[str]
    device_version: Optional[str]
    device_full_name: Optional[str]
