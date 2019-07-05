# -*- coding: utf-8 -*-

"""Main module."""
from pyarrow import flight
from pyarrow.compat import tobytes
import pyarrow as pa
import base64
from .command_pb2 import Command

class HttpDremioClientAuthHandler(flight.ClientAuthHandler):

    def __init__(self, username, password):
        super().__init__()
        self.username = tobytes(username)
        self.password = tobytes(password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        outgoing.write(base64.b64encode(self.username + b':' + self.password))
        self.token = incoming.read()

    def get_token(self):
        return self.token


def connect(hostname='localhost', port=47470, username='dremio', password='dremio123'):
    """
    Connect to and authenticate against Dremio's arrow flight server. Auth is skipped if username is None

    :param hostname: Dremio coordinator hostname
    :param port: Dremio coordinator port
    :param username: Username on Dremio
    :param password: Password on Dremio
    :return: arrow flight client
    """
    c = flight.FlightClient.connect('grpc+tcp://{}:{}'.format(hostname, port))
    if username:
        c.authenticate(HttpDremioClientAuthHandler(tobytes(username), tobytes(password if password else '')))
    return c


def query(sql, client=None, hostname='localhost', port=47470, username='dremio', password='dremio123', pandas=True):
    """
    Run an sql query against Dremio and return a pandas dataframe or arrow table

    Either host,port,user,pass tuple or a pre-connected client should be supplied. Not both

    :param sql: sql query to execute on dremio
    :param client: pre-connected client (optional)
    :param hostname: Dremio coordinator hostname (optional)
    :param port: Dremio coordinator port (optional)
    :param username: Username on Dremio (optional)
    :param password: Password on Dremio (optional)
    :param pandas: return a pandas dataframe (default) or an arrow table
    :return:
    """
    if not client:
        client = connect(hostname, port, username, password)

    cmd = Command(query=sql, parallel=False, coalesce=False, ticket=b'')
    info = client.get_flight_info(flight.FlightDescriptor.for_command(cmd.SerializeToString()))
    reader = client.do_get(info.endpoints[0].ticket)
    batches = []
    while True:
        try:
            batch, metadata = reader.read_chunk()
            batches.append(batch)
        except StopIteration:
            break
    data = pa.Table.from_batches(batches)
    if pandas:
        return data.to_pandas()
    else:
        return data
