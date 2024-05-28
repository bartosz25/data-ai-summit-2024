from datetime import datetime
from typing import List, Dict, Optional

from pyspark import Row
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout

from test.dataset_holder import DataGeneratorWrapper
from test.models import UserContext, TechnicalContext, Visit, VisitContext, Device, SessionPage, Session
from visits_generator.stateful_mapper import get_session_state_schema


def technical_context(browser='Firefox', browser_version='3.2.1', network_type='wifi',
                      device_type='pc', device_version='1.2.3') -> TechnicalContext:
    return TechnicalContext(
        browser=browser, browser_version=browser_version, network_type=network_type,
        device_type=device_type, device_version=device_version
    )


def user_context(ip='1.1.1.1', login='user A login', connected_since='2024-01-05T11:00:00.000Z') -> UserContext:
    return UserContext(
        ip=ip, login=login, connected_since=datetime.strptime(connected_since, '%Y-%m-%dT%H:%M:%S.%fZ'),
    )


def visit(visit_id='visit_1', event_time='2024-01-05T10:00:00.000Z', user_id='user A id',
          page='page1.html', referral='search', ad_id='ad 1',
          user_cxt=user_context(), technical_cxt=technical_context()) -> Visit:
    generated_visit = Visit(
        visit_id=visit_id, event_time=event_time,
        user_id=user_id, page=page, context=VisitContext(
            referral=referral, ad_id=ad_id, user=user_cxt, technical=technical_cxt
        )
    )
    DataGeneratorWrapper.add_visit(generated_visit)
    return generated_visit


def device(device_type='pc', version='1.2.3', full_name='PC v1.2.3 pro') -> Device:
    generated_device = Device(
        type=device_type, version=version, full_name=full_name
    )
    DataGeneratorWrapper.add_device(generated_device)
    return generated_device


def common_devices() -> List[Device]:
    return [device(device_type='pc', version='1.2.3', full_name='PC version 1.2.3-beta'),
            device(device_type='pc', version='4.5.6', full_name='PC version 4.5.6-beta')
            ]


def groupstate_for_test(event_time_watermark: int = 0,
                        watermark_present: bool = False,
                        timeout_timestamp: int = 0,
                        state_defined: bool = False) -> GroupState:
    return GroupState(
        hasTimedOut=False,
        optionalValue=Row(user_id=None, pages=None, device_type=None, device_version=None),
        batchProcessingTimeMs=0, eventTimeWatermarkMs=event_time_watermark,
        timeoutConf=GroupStateTimeout.EventTimeTimeout,
        watermarkPresent=watermark_present, defined=state_defined, updated=False, removed=False,
        timeoutTimestamp=timeout_timestamp,
        keyAsUnsafe=None, valueSchema=get_session_state_schema()
    )


def state_page_visit_row(page: str, event_time_as_milliseconds: int) -> Row:
    return Row(page=page, event_time_as_milliseconds=event_time_as_milliseconds)


def visited_page_for_stateful_of_user_A(page: str, event_time: str) -> Dict[str, str]:
    return {'page': page, 'event_time': event_time, 'user_id': 'user A', 'device_type': 'pc', 'device_version': 'pc_v_1'}


def session_page(page: str, visit_time: str) -> SessionPage:
    return SessionPage(
        page=page,
        visit_time=datetime.strptime(visit_time, '%Y-%m-%dT%H:%M:%S.%fZ')
    )


def session(visit_id: str = 'visit_1', user_id: str = 'user A', device_full_name: str = 'pc v1',
            device_type: str = 'pc', device_version: str = 'v1', pages: Optional[List[SessionPage]] = None) -> Session:
    pages = [session_page('page 1', '2024-01-05T10:00:00.000Z'),
             session_page('page 2', '2024-01-05T10:02:00.000Z')] if pages is None else pages

    sorted_pages = sorted(pages, key=lambda page: page.visit_time)
    return Session(
        visit_id=visit_id, user_id=user_id,
        start_time=sorted_pages[0].visit_time,
        end_time=sorted_pages[len(sorted_pages)-1].visit_time,
        device_type=device_type, device_version=device_version,
        device_full_name=device_full_name, visited_pages=pages
    )
