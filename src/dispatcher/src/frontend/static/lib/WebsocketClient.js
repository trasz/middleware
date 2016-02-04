/**
 * WebSocketClient
 * @authors Xin Tan (xt@ixsystems.com)
 * @date    2016-02-03 15:29:53
 * @version $0.1$
 */

 function WebSocketClient (host_addr) {
 	// body...
	this.connect = function() {
        this.socket = new WebSocket(`ws://host_addr:5000/socket`);
        this.socket.onmessage = this.__onmessage.bind(this);
        this.socket.onopen = this.__onopen.bind(this);
        this.socket.onclose = this.__onclose.bind(this);

        if (this.reconnectTimer) {
            clearTimeout(this.reconnectTimer);
            this.reconnectTimer = null;
        }
    }
	this.call = function(method, args, callback) {
        var id = DispatcherClient.__uuid();
        var timeout = setTimeout(() => { this.__ontimeout(id); }, this.defaultTimeout * 1000);
        var payload = {
            "method": method,
            "args": args
        };

        this.pendingCalls.set(id, {
            "method": method,
            "args": args,
            "callback": callback,
            "timeout": timeout
        });

        this.socket.send(this.__pack("rpc", "call", payload, id));
    }
    this.__uuid = function() {
        return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace( /[xy]/g, c => {
            var r = Math.random() * 16 | 0, v = c == "x" ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }
    this.__pack = function(namespace, name, args, id){
        return JSON.stringify({
            "namespace": namespace,
            "id": id || DispatcherClient.__uuid(),
            "name": name,
            "args": args
        });
    }
    this.__ontimeout == function(id) {
        let call = this.pendingCalls.get(id);
        let errno = getCode("ETIMEDOUT");

        call.callback(new RPCException(
            errno.code,
            errno.description
        ));

        this.pendingCalls.delete(id);
        this.onError(RPC_CALL_TIMEOUT, call.method, call.args);
    }
 }

