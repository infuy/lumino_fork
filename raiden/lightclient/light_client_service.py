from raiden.storage.sqlite import SerializedSQLiteStorage
from .light_client_data import LightClientData
from raiden.utils.typing import List


class LightClientService:
    def __init__(self, storage : SerializedSQLiteStorage):
        self._storage = storage
        self._light_clients_data = List[LightClientData]

    def get_light_clients_data(self) -> List[LightClientData]:
        light_clients = self._storage.query_light_clients()
        result = []
        if light_clients is not None and light_clients:
            result = [LightClientData(lc[0], lc[1]) for lc in light_clients]
        return result

    def fetch_light_clients_data(self):
        self._light_clients_data = self.get_light_clients_data()

    @property
    def light_clients_data(self):
        return self._light_clients_data

