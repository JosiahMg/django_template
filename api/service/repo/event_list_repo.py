# -*- coding: utf-8 -*-
# Author: Lx
# Date: 2021/3/16 18:36

from enum import Enum


class EventList(Enum):
    eventId = 'event_id'
    eventName = 'event_name'
    stationId = 'station_id'
    stationName = 'station_name'
    operationStatus = 'operate_status'

    voltageLevel = 'voltage_level_name'
    eventLevel = 'fault_level'
    eventType = 'fault_type'
    beginTime = 'begin_time'
    duration = 'duration'
    errorCause = 'error_post_cause'


class SignalList(Enum):
    act = 'status'
    time = 'occur_time'
    content = 'content'
    relevant = 'relevant'


class Substation(Enum):
    stationId = 'substation_id'
    stationName = 'substation_name'
    stationTypeId = 'area_id'
    stationTypeName = 'area_name'
    stationSelected = 'selected'


class FaultInfo(Enum):
    eventType = 'fault_type'


class FaultInfo1(Enum):
    eventId = 'event_id'
    eventName = 'fault_name'
    time = 'begin_time'
    stationId = 'station_id'
    stationName = 'station_name'
    eventTypeName = 'fault_type'




