import os
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

        self.eof_exchanges = []
        for i in range(SUM_AMOUNT):
            if i == ID:
                continue

            eof_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_PREFIX, [f"{SUM_PREFIX}_{i}"]
            )
            self.eof_exchanges.append(eof_exchange)

        self.fruit_by_req = {}
        self.fruit_by_req_lock = threading.Lock()

    def _publish_data(self, req_id, amount_by_fruit):
        logging.info(f"Broadcast data message for req_id={req_id}")

        for final_fruit_item in amount_by_fruit.values():
            for data_output_exchange in self.data_output_exchanges:
                data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [req_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

    def _publish_eof(self, req_id, exchanges):
        logging.info(f"Broadcast EOF message for req_id={req_id}")
        for exchange in exchanges:
            exchange.send(message_protocol.internal.serialize([req_id]))
 
    def _process_data(self, req_id, fruit, amount):
        with self.fruit_by_req_lock:
            logging.info(f"Process data for req_id={req_id}")

            amount_by_fruit = self.fruit_by_req.get(req_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

            self.fruit_by_req[req_id] = amount_by_fruit

    def _process_eof(self, req_id):
        logging.info(f"Process EOF for req_id={req_id}")

        with self.fruit_by_req_lock:
            amount_by_fruit = self.fruit_by_req.get(req_id)
            if not amount_by_fruit:
                return

            self._publish_data(req_id, amount_by_fruit)
            self._publish_eof(req_id, self.data_output_exchanges)

            del self.fruit_by_req[req_id]

        self._publish_eof(req_id, self.eof_exchanges)

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        threading.Thread(
            target=self.eof_exchange.start_consuming, 
            args=(self.process_data_messsage,)
        ).start()

        self.input_queue.start_consuming(self.process_data_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0

if __name__ == "__main__":
    main()
