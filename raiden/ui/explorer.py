import requests

explorer_endpoint = "http://localhost:8080/"


def register(rsk_address, rns_domain):
        r = requests.post(explorer_endpoint + 'luminoNode/', json={'node_address': rsk_address, 'rns_address': rns_domain})
        if r.status_code == 200:
            print("Succesfully registered into Lumino Explorer")
        else:
            print("There was an error registering into Lumino Explorer. Status: " + r.status_code)

