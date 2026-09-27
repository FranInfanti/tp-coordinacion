import bisect

from common import fruit_item

class FruitTop:

    def __init__(self, top_size, eof_amount):
        self.top = []
        self.eof = 0

        self.top_size = top_size
        self.eof_amount = eof_amount

    def upsert(self, fruit, amount):
        for i in range(len(self.top)):
            if self.top[i].fruit == fruit:
                updated_fruit = self.top[i] + fruit_item.FruitItem(fruit, amount)
                self.top.pop(i)

                bisect.insort(self.top, updated_fruit)
                return

        bisect.insort(self.top, fruit_item.FruitItem(fruit, amount))

    def is_complete(self):
        return self.eof == self.eof_amount

    def get_top(self):
        fruit_chunk = self.top[-self.top_size:]
        fruit_chunk.reverse()

        return list(
            map(
                lambda fruit_item: (fruit_item.fruit, fruit_item.amount),
                fruit_chunk,
            )
        )
