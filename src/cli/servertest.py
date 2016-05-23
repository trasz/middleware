import logging
from freenas.dispatcher.server import Server
from freenas.dispatcher.rpc import RpcContext, RpcService

logging.basicConfig(level=logging.DEBUG)
logging.debug('test')

class TestService(RpcService):
	def hello():
		print("test")
		return "world"


rpc = RpcContext()
rpc.register_service_instance('helloservice', TestService())
server = Server()
server.start('unix:///Users/jceel/test.sock', rpc)

