from common import fruit_item

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
