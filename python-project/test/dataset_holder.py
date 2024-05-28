from typing import List

from test.models import Visit, Device


class DataGeneratorWrapper:

    _visits = []
    _devices = []

    def __init__(self):
        self.container = []

    @classmethod
    def add_visit(cls, visit: Visit):
        cls._visits.append(visit)

    @classmethod
    def add_device(cls, device: Device):
        cls._devices.append(device)

    @classmethod
    def visits(cls) -> List[Visit]:
        return cls._visits

    @classmethod
    def devices(cls) -> List[Device]:
        return cls._devices
