import unittest
from unittest.mock import MagicMock

import order_book

BUY = True
SELL = False


class DummyTradeCSVFile(object):
    def __init__(self):
        self.dummy_file = []
        self._fieldnames = ['Buyer', 'Seller', 'Item', 'Quantity', 'Price']
        self._row_fieldnames = None

    @property
    def fieldnames(self):
        return self._fieldnames

    def writerows(self, rows):
        self._row_fieldnames = list(rows[0].keys())
        self.dummy_file.extend([list(r.values()) for r in rows])

    def get_rows(self):
        return self.dummy_file

    @property
    def row_fieldnames(self):
        return self._row_fieldnames


class TradesSameOrderAsOrdersTest(unittest.TestCase):
    def test_enter_order(self):
        trade_csv_file = MagicMock()
        ibm_book = order_book.OrderBook('IBM', trade_csv_file)
        bid = order_book.Order(10, 100, 'Richard', ibm_book.get_sequence_number())
        ibm_book.match(bid, BUY)
        self.assertTrue(not trade_csv_file.called)
        self.assertEqual(ibm_book.depth, 1)

    def test_match_orders(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        bid = order_book.Order(10, 100, 'Richard', ibm_book.get_sequence_number())
        ibm_book.match(bid, BUY)
        ask = order_book.Order(10, 90, 'Henry', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        self.assertEqual(ibm_book.depth, 0)
        match_rows = dummy_csv_file.get_rows()
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0], ['Richard', 'Henry', 'IBM', 10, 100])

    def test_order_sequence(self):
        dummy_csv_file_forward = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file_forward)
        bid1 = order_book.Order(10, 100, 'Richard', ibm_book.get_sequence_number())
        ibm_book.match(bid1, BUY)
        bid2 = order_book.Order(20, 100, 'Bernadine', ibm_book.get_sequence_number())
        ibm_book.match(bid2, BUY)
        ask = order_book.Order(10, 90, 'Henry', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        self.assertEqual(ibm_book.depth, 1)
        match_rows = dummy_csv_file_forward.get_rows()
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0], ['Richard', 'Henry', 'IBM', 10, 100])

    def test_order_sequence_reversed(self):
        dummy_csv_file_backward = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file_backward)
        bid1 = order_book.Order(20, 100, 'Bernadine', ibm_book.get_sequence_number())
        ibm_book.match(bid1, BUY)
        bid2 = order_book.Order(10, 100, 'Richard', ibm_book.get_sequence_number())
        ibm_book.match(bid2, BUY)
        ask = order_book.Order(10, 90, 'Henry', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        self.assertEqual(ibm_book.depth, 2)
        match_rows = dummy_csv_file_backward.get_rows()
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0], ['Bernadine', 'Henry', 'IBM', 10, 100])


class TradesColumnsTest(unittest.TestCase):
    def test_trades_columns(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        bid = order_book.Order(10, 100, 'Richard', ibm_book.get_sequence_number())
        ibm_book.match(bid, BUY)
        ask = order_book.Order(10, 90, 'Henry', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        self.assertEqual(dummy_csv_file.row_fieldnames, ['Buyer', 'Seller', 'Item', 'Quantity', 'Price'])


class TestPriceTimePriority(unittest.TestCase):
    def test_price_time_priority(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        bid1 = order_book.Order(10, 100, 'Customer1', ibm_book.get_sequence_number())
        ibm_book.match(bid1, BUY)
        bid2 = order_book.Order(10, 101, 'Customer2', ibm_book.get_sequence_number())
        ibm_book.match(bid2, BUY)
        ask = order_book.Order(5, 99, 'Seller1', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        self.assertEqual(ibm_book.depth, 2)
        match_rows = dummy_csv_file.get_rows()
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0], ['Customer2', 'Seller1', 'IBM', 5, 101])

    def test_price_time_priority_reversed_order(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        bid1 = order_book.Order(10, 101, 'Customer2', ibm_book.get_sequence_number())
        ibm_book.match(bid1, BUY)
        bid2 = order_book.Order(10, 100, 'Customer1', ibm_book.get_sequence_number())
        ibm_book.match(bid2, BUY)
        ask = order_book.Order(5, 99, 'Seller1', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        self.assertEqual(ibm_book.depth, 2)
        match_rows = dummy_csv_file.get_rows()
        self.assertEqual(len(match_rows), 1)
        self.assertEqual(match_rows[0], ['Customer2', 'Seller1', 'IBM', 5, 101])


class TestHitBookPrice(unittest.TestCase):
    def test_hit_100_bid(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        bid = order_book.Order(10, 100, 'Customer1', ibm_book.get_sequence_number())
        ibm_book.match(bid, BUY)
        ask = order_book.Order(10, 2, 'Seller1', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        match_rows = dummy_csv_file.get_rows()
        self.assertEqual(match_rows[0], ['Customer1', 'Seller1', 'IBM', 10, 100])

    def test_hit_2_ask(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        ask = order_book.Order(10, 2, 'Seller1', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        bid = order_book.Order(10, 100, 'Customer1', ibm_book.get_sequence_number())
        ibm_book.match(bid, BUY)
        match_rows = dummy_csv_file.get_rows()
        self.assertEqual(match_rows[0], ['Customer1', 'Seller1', 'IBM', 10, 2])


class TestMarketOrders(unittest.TestCase):
    def test_market_ask(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        for bid in [order_book.Order(10, 100 + n, 'Customer1', ibm_book.get_sequence_number()) for n in range(10)]:
            ibm_book.match(bid, BUY)
        ask = order_book.Order(100, 1000000000, 'Seller1', ibm_book.get_sequence_number())
        ibm_book.match(ask, SELL)
        match_rows = dummy_csv_file.get_rows()
        for n, match in enumerate(match_rows):
            self.assertEqual(match, ['Customer1', 'Seller1', 'IBM', 10, 100 + n - 1])

    def test_market_bid(self):
        dummy_csv_file = DummyTradeCSVFile()
        ibm_book = order_book.OrderBook('IBM', dummy_csv_file)
        for ask in [order_book.Order(10, 100 + n, 'Seller1', ibm_book.get_sequence_number()) for n in range(10)]:
            ibm_book.match(ask, SELL)
        bid = order_book.Order(100, 0, 'Customer1', ibm_book.get_sequence_number())
        ibm_book.match(bid, BUY)
        match_rows = dummy_csv_file.get_rows()
        for n, match in enumerate(match_rows):
            self.assertEqual(match, ['Customer1', 'Seller1', 'IBM', 10, 100 + n - 1])


if __name__ == '__main__':
    unittest.main()
