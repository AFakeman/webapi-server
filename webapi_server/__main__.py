from webapi_server.server import Server

if __name__ == "__main__":
    server = Server(config="config.yaml")
    server.run()
