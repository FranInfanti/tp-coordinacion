from common import message_protocol


class MessageHandler:

    def __init__(self):
        self.req_id = id(self)
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.req_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.req_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        req_id = fields.pop(0)
        fruit_top = fields.pop(0)

        if req_id != self.req_id:
            return None

        return fruit_top
