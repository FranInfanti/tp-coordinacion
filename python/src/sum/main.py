import os
import signal
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
_KILL = 5

_TIMEOUT = 4.0

class FruitAmount:

    def __init__(self):
        self.total_req_count = None
        self.req_count = 0
        self.amount_by_fruit = {}

    def upsert(self, fruit, amount):
        self.req_count += 1
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def is_complete(self, extra_req_count):
        total_req_count = self.req_count + extra_req_count
        return self.total_req_count is not None and total_req_count == self.total_req_count

def define_exchanges(host, prefix, amount, exclude_id = None):
    exchanges = {}

    for i in range(amount):
        if i == exclude_id:
            continue

        exchanges[i] = middleware.MessageMiddlewareExchangeRabbitMQ(
            host, prefix, [f"{prefix}_{i}"]
        )

    return exchanges

class SumFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )

        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{ID}"]
        )

        self.output_exchanges = define_exchanges(
            MOM_HOST, AGGREGATION_PREFIX, AGGREGATION_AMOUNT
        )

        self.eof_exchanges = define_exchanges(
            MOM_HOST, SUM_PREFIX, SUM_AMOUNT, exclude_id=ID
        )

        self.fruit_amount = {}
        self.fruit_amount_lock = threading.Lock()

        self.ok_amount = {}

        self.exchange_consumer = None

        signal.signal(signal.SIGTERM, self._sigterm_handler)

    def _sigterm_handler(self, signum, frame):
        logging.info("SIGTERM received, proceed with graceful shutdown...")

        # stop consuming from the input queue
        try:
            self.input_queue.stop_consuming()
        except Exception:
            pass

        try:
            self.input_queue.close()
        except Exception:
            pass

        try:
            self.input_exchange.send(
                message_protocol.internal.serialize([_KILL])
            )
        except Exception as error:
            logging.warning(f"Unable to notify exchange consumer during shutdown: {error}")

        if self.exchange_consumer is not None:
            self.exchange_consumer.join(timeout=_TIMEOUT)
            if self.exchange_consumer.is_alive():
                logging.warning("Exchange consumer did not stop before timeout")

        exchanges = [self.input_exchange]
        exchanges.extend(self.output_exchanges.values())
        exchanges.extend(self.eof_exchanges.values())

        for exchange in exchanges:
            try:
                exchange.close()
            except Exception:
                pass

    def _get_aggregation_node(self, req_id, fruit):
        hash = hashlib.md5(f"{req_id}:{fruit}".encode()).hexdigest()
        return int(hash, 16) % AGGREGATION_AMOUNT

    def _publish(self, message, exchanges):
        for exchange in exchanges:
            exchange.send(
                message_protocol.internal.serialize(message)
            )

    def _send_data(self, req_id, amount_by_fruit):
        logging.info(f"Sending DATA message for req_id={req_id}")

        for final_fruit_item in amount_by_fruit.values():
            i = self._get_aggregation_node(req_id, final_fruit_item.fruit)
            data_output_exchange = self.output_exchanges[i]

            data_output_exchange.send(
                message_protocol.internal.serialize(
                    [_DATA, req_id, final_fruit_item.fruit, final_fruit_item.amount]
                )
            )

    def _send_eof(self, req_id):
        logging.info(f"Sending EOF message for req_id={req_id}")

        self._publish(
            [_EOF, req_id], self.output_exchanges.values()
        )

    def _check_req_count(self, req_id, extra_req_count):
        logging.info(f"Checking total request count for req_id={req_id}")

        with self.fruit_amount_lock:
            fruit_amount = self.fruit_amount.get(req_id)
            if not fruit_amount:
                logging.error(f"Unexpected error for req_id={req_id}")
                return   

            if fruit_amount.is_complete(extra_req_count):
                self._send_data(req_id, fruit_amount.amount_by_fruit)
                self._send_eof(req_id)

                logging.info(f"Sending COMMIT message for req_id={req_id}")
                self._publish([_COMMIT, req_id], self.eof_exchanges.values())

                del self.fruit_amount[req_id]
            else:
                logging.warning(f"Total count mismatch for req_id={req_id}")
                logging.info(f"Sending PREPARE for req_id={req_id}")
                
                self._publish([_PREPARE, req_id, ID], self.eof_exchanges.values())

    def _process_kill(self):
        try:
            self.input_exchange.stop_consuming()
        except Exception:
            pass

    def _process_commit(self, req_id):
        logging.info(f"Process COMMIT message for req_id={req_id}")

        with self.fruit_amount_lock:
            fruit_amount = self.fruit_amount.get(req_id, FruitAmount())

            self._send_data(req_id, fruit_amount.amount_by_fruit)
            self._send_eof(req_id)

            self.fruit_amount.pop(req_id, None)

    def _process_ok(self, req_id, req_count):
        logging.info(f"Process OK message for req_id={req_id}")

        ok_amount = self.ok_amount.get(req_id, (0, 0))
        ok_amount = (ok_amount[0] + 1, ok_amount[1] + req_count)

        self.ok_amount[req_id] = ok_amount

        ok_count_recv = ok_amount[0]
        req_count_recv = ok_amount[1]
        if ok_count_recv == SUM_AMOUNT - 1:
            self._check_req_count(req_id, req_count_recv)

            # even if we don't send the data, we still need to erase the OK count
            del self.ok_amount[req_id]

    def _process_prepare(self, req_id, id):
        logging.info(f"Process PREPARE message for req_id={req_id}")

        with self.fruit_amount_lock:
            # if we don't have the request, we still need to send an OK with 0 count
            fruit_amount = self.fruit_amount.get(req_id, FruitAmount())

            id_exchange = self.eof_exchanges.get(id)
            if not id_exchange:
                logging.error(f"Exchange not found for id={id}")
                return

            logging.info(f"Sending OK message for req_id={req_id} to id={id}")
            self._publish([_OK, req_id, fruit_amount.req_count], [id_exchange])

    def _process_eof(self, req_id, total_req_count):
        logging.info(f"Process EOF message for req_id={req_id}")

        with self.fruit_amount_lock:
            fruit_amount = self.fruit_amount.get(req_id, FruitAmount())
            fruit_amount.total_req_count = total_req_count

            self.fruit_amount[req_id] = fruit_amount

            if fruit_amount.is_complete(0):
                self._send_data(req_id, fruit_amount.amount_by_fruit)
                self._send_eof(req_id)

                logging.info(f"Sending COMMIT message for req_id={req_id}")
                self._publish([_COMMIT, req_id], self.eof_exchanges.values())
                del self.fruit_amount[req_id]
                
                return

        logging.info(f"Sending PREPARE message for req_id={req_id}")
        self._publish([_PREPARE, req_id, ID], self.eof_exchanges.values())

    def _process_data(self, req_id, fruit, amount):
        logging.info(f"Process DATA message for req_id={req_id}")

        with self.fruit_amount_lock:
            fruit_amount = self.fruit_amount.get(req_id, FruitAmount())
            fruit_amount.upsert(fruit, amount)

            self.fruit_amount[req_id] = fruit_amount

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
        elif opcode == _KILL:
            self._process_kill()
            return

        ack()

    def start(self):
        self.exchange_consumer = threading.Thread(
            target=self.input_exchange.start_consuming, 
            args=(self.process_message,)
        )

        self.exchange_consumer.start()

        try:
            self.input_queue.start_consuming(self.process_message)
        except Exception as e:
            logging.error(f"Error while consuming from the queue: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
