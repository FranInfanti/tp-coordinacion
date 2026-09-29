import os
import signal
import logging

from common import middleware, message_protocol, utils

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

_EOF = 0
_DATA = 1

class JoinFilter:

    def __init__(self):
        signal.signal(signal.SIGTERM, self._sigterm_handler)

        self.sigterm_recv = 0

        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top = {}

    def _sigterm_handler(self, signum, frame):
        logging.info("SIGTERM received, proceed with graceful shutdown...")

        self.sigterm_recv = 1

        try:
            self.input_queue.stop_consuming()
        except Exception:
            logging.error("Error while trying to stop consuming from the input queue")

    def _process_data(self, req_id, fruit, amount):
        logging.info(f"Processing DATA message for req_id={req_id}")

        fruit_top = self.fruit_top.get(
            req_id, utils.FruitTop(TOP_SIZE, AGGREGATION_AMOUNT)
        )
        fruit_top.upsert(fruit, amount)

        self.fruit_top[req_id] = fruit_top

    def _process_eof(self, req_id):
        logging.info(f"Process EOF message for req_id={req_id}")

        fruit_top = self.fruit_top.get(
            req_id, utils.FruitTop(TOP_SIZE, AGGREGATION_AMOUNT)
        )
        fruit_top.eof += 1
        self.fruit_top[req_id] = fruit_top

        if not fruit_top.is_complete():
            logging.info(f"Still waiting for more DATA message for req_id={req_id}")
            return

        logging.info(f"Sending fruit top for req_id={req_id}")
        fruit_top = fruit_top.get_top()
        self.output_queue.send(
            message_protocol.internal.serialize([req_id, fruit_top])
        )

        del self.fruit_top[req_id]

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        opcode = fields.pop(0)

        if opcode == _DATA:
            self._process_data(*fields)
        elif opcode == _EOF:
            self._process_eof(*fields)

        ack()

    def start(self):
        try:
            self.input_queue.start_consuming(self.process_message)
        except Exception as e:
            if self.sigterm_recv == 0:
                raise e
            else:
                logging.info("Interrupted by SIGTERM")
        finally:
            self.input_queue.close()
            self.output_queue.close()

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    
    try:
        join_filter.start()
    except Exception as e:
        logging.error(e)
        return 2
    
    return 0

if __name__ == "__main__":
    main()
