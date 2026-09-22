import os
import hashlib
import logging
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

_EOF = 0
_DATA = 1
_PREPARE = 2
_OK = 3
_COMMIT = 4

class Fruits:
    def __init__(self, req_id):
        self.req_id = req_id
        self.count = 0
        self.total_count = None
        self.amount_by_fruit = {}

    def upsert(self, fruit, amount):
        self.count += 1
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        )

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.eof_exchanges = {}
        for i in range(SUM_AMOUNT):
            if i == ID:
                continue

            eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{i}"]
            )

            self.eof_exchanges[i] = eof_exchange

        self.fruits_by_req = {}
        self.fruit_by_req_lock = threading.Lock()

        self.ok_recv = {}

    def _get_aggregation_node(self, req_id):
        hash = hashlib.md5(str(req_id).encode()).hexdigest()
        return int(hash, 16) % AGGREGATION_AMOUNT

    def _send_data(self, req_id, amount_by_fruit):
        logging.info(f"Sending data message for req_id={req_id}")

        i = self._get_aggregation_node(req_id)
        for final_fruit_item in amount_by_fruit.values():
            data_output_exchange = self.data_output_exchanges[i]
            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [req_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

    def _send_eof(self, req_id):
        logging.info(f"Sending EOF message for req_id={req_id}")

        i = self._get_aggregation_node(req_id)
        self.data_output_exchanges[i].send(message_protocol.internal.serialize([req_id]))


    def _publish(self, message, exchanges):
        for exchange in exchanges:
            exchange.send(message_protocol.internal.serialize(message))

    def _check_total_count(self, req_id, extra_total_count):
        logging.info(f"Checking total count for req_id={req_id}")

        with self.fruit_by_req_lock:
            fruits = self.fruits_by_req.get(req_id, Fruits(req_id))

            if fruits.count + extra_total_count == fruits.total_count:
                self._send_data(req_id, fruits.amount_by_fruit)
                self._send_eof(req_id)

                logging.info(f"Sending COMMIT for req_id={req_id}")
                self._publish([_COMMIT, req_id], self.eof_exchanges.values())

                del self.fruits_by_req[req_id]
            else:
                logging.info(f"Total count mismatch for req_id={req_id}, expected {fruits.total_count}, got {fruits.count + extra_total_count}")
                logging.info(f"Sending PREPARE for req_id={req_id}")
                self._publish([_PREPARE, req_id, ID], self.eof_exchanges.values())

    def _process_commit(self, req_id):
        logging.info(f"Process COMMIT for req_id={req_id}")

        fruits = self.fruits_by_req.get(req_id, Fruits(req_id))

        self._send_data(req_id, fruits.amount_by_fruit)
        self._send_eof(req_id)

        del self.fruits_by_req[req_id]

    def _process_ok(self, req_id, count):
        logging.info(f"Process OK for req_id={req_id}")

        ok_recv = self.ok_recv.get(req_id, (0, 0))
        ok_recv = (ok_recv[0] + 1, ok_recv[1] + count)

        self.ok_recv[req_id] = ok_recv

        total_recv = ok_recv[0]
        total_count = ok_recv[1]
        if total_recv == SUM_AMOUNT - 1:
            self._check_total_count(req_id, total_count)
            del self.ok_recv[req_id]

    def _process_prepare(self, req_id, id):
        logging.info(f"Process PREPARE for req_id={req_id}")

        with self.fruit_by_req_lock:
            fruits = self.fruits_by_req.get(req_id, Fruits(req_id))

            id_exchange = self.eof_exchanges.get(id)
            if id_exchange is None:
                logging.error(f"Exchange not found for id={id}")
                return

            logging.info(f"Sending OK for req_id={req_id} to id={id}")
            self._publish([_OK, req_id, fruits.count], [id_exchange])

    def _process_eof(self, req_id, total_count):
        logging.info(f"Process EOF for req_id={req_id}")

        with self.fruit_by_req_lock:
            fruits = self.fruits_by_req.get(req_id, Fruits(req_id))
            fruits.total_count = total_count

            self.fruits_by_req[req_id] = fruits

        if SUM_AMOUNT == 1:
            self._check_total_count(req_id, 0)
            return

        logging.info(f"Sending PREPARE for req_id={req_id}")
        self._publish([_PREPARE, req_id, ID], self.eof_exchanges.values())

    def _process_data(self, req_id, fruit, amount):
        logging.info(f"Process data for req_id={req_id}")

        with self.fruit_by_req_lock:
            fruits = self.fruits_by_req.get(req_id, Fruits(req_id))
            fruits.upsert(fruit, amount)

            self.fruits_by_req[req_id] = fruits

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        opcode = fields.pop(0)

        if opcode == _DATA:
            self._process_data(*fields)
        elif opcode == _EOF:
            self._process_eof(*fields)
        elif opcode == _PREPARE:
            self._process_prepare(*fields)
        elif opcode == _OK:
            self._process_ok(*fields)
        elif opcode == _COMMIT:
            self._process_commit(*fields)

        ack()

    def start(self):
        threading.Thread(
            target=self.eof_exchange.start_consuming, 
            args=(self.process_message,)
        ).start()

        self.input_queue.start_consuming(self.process_message)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
