from raiden.storage.wal import WriteAheadLog
from .light_client_data import LightClientData
from raiden.utils.typing import List


class LightClientService:
    def __init__(self, wal : WriteAheadLog):
        self.wal = wal

    def get_light_clients_data(self) -> List[LightClientData]:
        light_clients = self.wal.storage.query_light_clients()
        result = []
        if light_clients is not None and light_clients:
            result = [LightClientData(lc[0], lc[1]) for lc in light_clients]
        return result
