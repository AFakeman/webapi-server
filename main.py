from server import Server

server = Server()

server.config_from_file("config.yaml")

server.run()