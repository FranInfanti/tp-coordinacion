import os
import signal
import bisect
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])

_EOF = 0
_DATA = 1

class FruitTop:

    def __init__(self):
        self.top = []
        self.eof = 0

    def upsert(self, fruit, amount):
        for i in range(len(self.top)):
            if self.top[i].fruit == fruit:
                updated_fruit = self.top[i] + fruit_item.FruitItem(fruit, amount)
                self.top.pop(i)

                bisect.insort(self.top, updated_fruit)
                return

        bisect.insort(self.top, fruit_item.FruitItem(fruit, amount))

    def is_top_complete(self):
        return self.eof == SUM_AMOUNT

    def get_top(self, top_size):
        fruit_chunk = self.top[-top_size:]
        fruit_chunk.reverse()

        return list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )

class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )

        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.fruit_top = {}
        self.eof_fruit_top = {}

        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def _sigterm_handler(self, signum, frame):
        logging.info("SIGTERM received, proceed with graceful shutdown...")

        try:
            self.input_exchange.stop_consuming()
            self.input_exchange.close()
            self.output_queue.close()
        except Exception:
            pass

    def _process_data(self, req_id, fruit, amount):
        logging.info(f"Processing DATA message for req_id={req_id}")

        fruit_top = self.fruit_top.get(req_id, FruitTop())
        fruit_top.upsert(fruit, amount)

        self.fruit_top[req_id] = fruit_top

    def _process_eof(self, req_id):
        logging.info(f"Process EOF message for req_id={req_id}")

        fruit_top = self.fruit_top.get(req_id, FruitTop())
        fruit_top.eof += 1

        self.fruit_top[req_id] = fruit_top
        if not fruit_top.is_top_complete():
            logging.info(f"Still waiting for more DATA message for req_id={req_id}")
            return

        logging.info(f"Sending fruit top for req_id={req_id}")
        fruit_top = fruit_top.get_top(TOP_SIZE)
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
            self.input_exchange.start_consuming(self.process_message)
        except Exception as e:
            logging.error(f"Error while consuming from the exchange: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0

if __name__ == "__main__":
    main()
