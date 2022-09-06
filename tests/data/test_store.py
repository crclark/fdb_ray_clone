from fdb_ray_clone.data.store import StoreServer, StoreClient

import pyarrow as pa


def table() -> pa.Table:
    n_legs = pa.array([2, 4, 5, 100])
    animals = pa.array(["Flamingo", "Horse", "Brittle stars", "Centipede"])
    names = ["n_legs", "animals"]
    return pa.Table.from_arrays([n_legs, animals], names=names)


def test_client_server() -> None:
    HOST = "localhost"
    PORT = 50000

    with StoreServer(HOST, PORT) as server:
        x = [1, 2, 3]
        sm_x = server.store_local(x)
        y = table()
        sm_y = server.store_local(y)
        client = StoreClient(HOST, PORT)

        assert client.get(sm_x.name) == x
        assert client.get(sm_y.name) == y
