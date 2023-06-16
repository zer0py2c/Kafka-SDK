import json


def msg_decode(msg):
    try:
        msg = msg.decode("utf-8")
        if "{" not in msg:
            return msg
        else:
            return json.loads(msg)
    except:
        return msgpack.unpackb(msg)


def msg_encode(msg):
    if isinstance(msg, str):
        return msg.encode("utf-8")
    elif isinstance(msg, dict):
        return json.dumps(msg).encode("utf-8")
