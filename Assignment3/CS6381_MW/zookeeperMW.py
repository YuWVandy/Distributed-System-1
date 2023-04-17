from kazoo.client import KazooClient

class ZooKeeperClient:
    def __init__(self, hosts="localhost:6666"):
        self.kz_client = KazooClient(hosts=hosts)
        self.kz_client.start()

    def create_node(self, path, data):
        self.kz_client.create(path, data.encode(), makepath=True)

    def get_node_data(self, path):
        data, _ = self.kz_client.get(path)
        return data.decode()

    def set_node_data(self, path, data):
        self.kz_client.set(path, data.encode())

    def delete_node(self, path):
        self.kz_client.delete(path, recursive=True)

    def close(self):
        self.kz_client.stop()
        self.kz_client.close()

if __name__ == "__main__":
    # set underlying default logging capabilities
    zk = ZooKeeperClient()
