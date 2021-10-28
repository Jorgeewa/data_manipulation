from abc import ABC, abstractmethod
from typing import List


class DerivedObservables(ABC):


    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def get_observables(self) -> List[str]:
        pass

# implementation
class Dobservables(DerivedObservables):

    def __init__(self, name: str, observables: List[str]):
        self.__name = name
        self.__observables = observables

    def name(self) -> str:
        return self.__name

    def get_observables(self) -> List[str]:
        return self.__observables


humidex = Dobservables("humidex", ["temperature", "humidity"])
heat_stress_index = Dobservables("heat_stress_index", ["temperature", "humidity"])
effective_temperature = Dobservables("effective_temperature", ["temperature", "humidity", "airpseed"])
digestion_index = Dobservables("digestion_index", ["good_droppings", "bad_droppings"])

derived_observables = [humidex, heat_stress_index, effective_temperature, digestion_index]




