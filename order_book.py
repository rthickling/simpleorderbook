import argparse
import csv
import logging
import os
import os.path
import platform
import time
from abc import abstractmethod
from operator import le, ge
from pathlib import Path

from sortedcontainers import SortedList, SortedKeyList

if platform.system() == 'Windows':
    import win32pipe
    import win32file
    import winerror


class OrderPipeReceiver(object):
    """For use with a streaming order simulator"""
    @classmethod
    def create_order_pipe_receiver(cls, pipe_name='orders.pipe'):
        if platform.system() == 'Windows':
            return WindowsOrderPipeReceiver(pipe_name)
        else:
            return UnixOrderPipeReceiver(pipe_name)

    @abstractmethod
    def get_line(self):
        """Gets a line from the pipe"""


class WindowsOrderPipeReceiver(OrderPipeReceiver):
    class WindowsOrderPipeFailure(Exception):
        pass

    def __init__(self, pipe_name):
        self.handle = win32file.CreateFile(
            r'\\.\pipe\\' + pipe_name, win32file.GENERIC_READ | win32file.GENERIC_WRITE, 0, None,
            win32file.OPEN_EXISTING, 0, None)
        if win32pipe.SetNamedPipeHandleState(self.handle, win32pipe.PIPE_READMODE_MESSAGE, None, None) == 0:
            raise self.WindowsOrderPipeFailure("SetNamedPipeHandleState failed")

    def get_line(self):
        failed, line = win32file.ReadFile(self.handle, 0x40 * 0x400)
        if failed:
            raise self.WindowsOrderPipeFailure("Couldn't read line")
        return line.decode('ascii').strip()


class UnixOrderPipeReceiver(OrderPipeReceiver):
    def __init__(self, pipe_name):
        pipe_path = f"./{pipe_name}"
        if not os.path.exists(pipe_path):
            os.mkfifo(pipe_path)
        self.pipe_in = open(pipe_path, 'r')

    def __del__(self):
        if hasattr(self, 'pipe_in') and self.pipe_in:
            self.pipe_in.close()

    def get_line(self):
        line_in = self.pipe_in.readline().strip()
        logging.debug(f"Read line: {line_in}")
        return line_in


def clear_path(path):
    if os.path.exists(path):
        os.remove(path)


class Order(object):
    """Represents an order - a bid or an ask - to be placed in a stock exchange order book"""
    def __init__(self, quantity, price, customer, sequence_number):
        self._quantity, self._price, self._customer, self._sequence_number = quantity, price, customer, sequence_number

    @property
    def price(self):
        return self._price

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, quantity):
        self._quantity = quantity

    @property
    def customer(self):
        return self._customer

    @property
    def sequence_number(self):
        return self._sequence_number

    def __lt__(self, other):
        return self.forward_key() < other.forward_key()

    def __eq__(self, other):
        return self.forward_key() == other.forward_key()

    def forward_key(self):
        return self._price, self._sequence_number, self._quantity  # Includes quantity to allow amendments in place

    def reverse_key(self):
        return -self._price, self._sequence_number, self._quantity

    def __str__(self):
        return (f"Quantity {self.quantity}, Price {self.price}, Customer {self.customer}, "
                f"Sequence Number {self._sequence_number}")


class AskSide(SortedList):
    pass


class BidSide(SortedKeyList):
    def __init__(self):
        super().__init__(key=Order.reverse_key)


class OrderBook(object):
    """Represents a stock exchange order book for a particular stock.  There is a Bid and Ask side"""
    def __init__(self, name, trade_csv_file):
        self.name = name
        self.trade_csv_file = trade_csv_file
        self.bids = BidSide()
        self.asks = AskSide()
        self.sequence_number = 0

    def get_sequence_number(self):
        self.sequence_number += 1
        return self.sequence_number

    @property
    def depth(self):
        return len(self.asks) + len(self.bids)

    def match(self, order, is_bid):
        """Match teh entered order with any matching orders already in the book.  Residual quantities on a partial fill
        are added to the book.
        When orders ar matched (bids with asks or vice versa) they become trades and are written to the trade file"""
        matches = []
        other_side, this_side, compare = (self.asks, self.bids, le) if is_bid else (self.bids, self.asks, ge)
        matched_orders_to_remove = []
        for other in other_side:  # Go down the orders
            if compare(other.price, order.price):
                customers = [order.customer, other.customer] if is_bid else [other.customer, order.customer]
                partial_fill = other.quantity < order.quantity
                quantity = other.quantity if partial_fill else order.quantity
                matches.append(customers + [self.name, quantity, other.price])
                if partial_fill:  # Entered order is partially filled
                    order.quantity -= other.quantity
                    matched_orders_to_remove.append(other)  # Other filled but don't disturb data while iterating
                else:
                    matched_orders_to_remove.append(other)  # Change quantity but don't disturb data while iterating
                    residual_quantity = other.quantity - order.quantity
                    order.quantity = 0
                    if residual_quantity:
                        other_side.add(Order(residual_quantity, other.price, other.customer, other.sequence_number))
                    break  # Fully matched
            else:
                break  # All further prices will fail to compare
        for matched_other in matched_orders_to_remove:
            other_side.remove(matched_other)  # Finished iterating - now apply changes to book data structure
        if order.quantity:  # New order or there's remainder after matching, add a new order to the book
            this_side.add(order)
        if matches:  # Write matches to trades file
            self.trade_csv_file.writerows([dict(zip(self.trade_csv_file.fieldnames, match)) for match in matches])


def read_streamed_orders(trades_file_name, pipe_name='order_pipe'):
    """Read orders from a stream - implemented as a pipe.  Tolerant to initial unavailability of pipe"""
    finished = False
    while not finished:
        try:
            order_pipe = OrderPipeReceiver.create_order_pipe_receiver(pipe_name)
            clear_path(trades_file_name)
            order_book = {}
            header = ['Customer', 'Item', 'Side', 'Quantity', 'Price']
            with open(trades_file_name, 'w') as trades_file:
                fieldnames = ['Buyer', 'Seller', 'Item', 'Quantity', 'Price']
                trade_csv_file = csv.DictWriter(trades_file, fieldnames, lineterminator='\r')
                trade_csv_file.writeheader()
                while True:
                    order_line = order_pipe.get_line()
                    if order_line:
                        order_data = dict(zip(header, [o.strip() for o in order_line.split(',')]))
                        place_order(order_book, order_data, trade_csv_file)
                    else:
                        finished = True
                        break
        except OSError as ose:
            if hasattr(ose, 'winerror'):  #
                if winerror == winerror.ERROR_FILE_NOT_FOUND:
                    logging.info("No pipe, trying again in one second")
                    time.sleep(1)
                elif winerror == winerror.ERROR_BROKEN_PIPE:
                    logging.error("Broken pipe: ending")
                    finished = True
            else:
                logging.error(f"Exception thrown: {ose.strerror}")
                finished = True
        except Exception as ex:
            logging.error(f"Exception: {repr(ex)}")
            finished = True


def read_file_orders(orders_file, trades_file_name):
    """REad orders from a file"""
    clear_path(trades_file_name)
    header = ['Buyer', 'Seller', 'Item', 'Quantity', 'Price']
    order_book = {}
    with open(trades_file_name, 'w') as trades_file:
        trade_csv_file = csv.DictWriter(trades_file, fieldnames=header, lineterminator='\r')
        trade_csv_file.writeheader()
        with open(orders_file) as orders_file:
            data_reader = csv.DictReader(orders_file)
            for order_data in data_reader:
                place_order(order_book, order_data, trade_csv_file)


def place_order(order_book, order_data, trade_csv_file):
    """Place an order in the appropriate order book and try to match it.  Results written to provided CSV file"""
    name = order_data['Item']
    if name not in order_book:
        order_book[name] = OrderBook(name, trade_csv_file)
    order = Order(int(order_data['Quantity']), int(order_data['Price']), order_data['Customer'].strip(),
                  order_book[name].get_sequence_number())
    order_book[name].match(order, order_data['Side'] == 'Buy')
    logging.debug(f"Order: {order}")
    logging.debug(f"Order book for {name} size: {order_book[name].depth}.")


def execute(arguments):
    if arguments.debug:
        logging.basicConfig(level=logging.DEBUG)
    if arguments.orders_file:
        read_file_orders(arguments.orders_file, arguments.trade_file)
    else:
        read_streamed_orders(arguments.trade_file, arguments.pipe_name)


def construct_arg_parser():
    p = argparse.ArgumentParser(description="Stock Market Order Book")
    group = p.add_mutually_exclusive_group()
    group.add_argument('-f', '--orders_file', metavar='path', required=False, help='path to a file of orders to submit')
    group.add_argument('-p', '--pipe_name', metavar='name', required=False, default='order_pipe',
                       help='name of pipe from client')
    p.add_argument('-t', '--trade_file', metavar='path', required=True,
                        help='path to file to write matched trades')
    p.add_argument('-D', '--debug', action='store_true', help='turn on DEBUG logging')

    return p


def main():
    """Provided as a convenience for systems running through main()
    Users should modify main() to suit their exact purposes with command-line strings here
    """
    p = construct_arg_parser()

    # Adjust this path as appropriate to piont to the directory containing orders files.
    order_book_path = Path("/home/richard/projects/OrderBook")

    # Uncomment the 'execute()' line below to allow orders to be streamed from an order simulator over a pipe
    piped_trades_file = order_book_path/"trades_from_pipe.csv"
    get_orders_from_pipe_command_line = f"-t {piped_trades_file} -p order_pipe"

    execute(p.parse_args(get_orders_from_pipe_command_line.split()))

    ###############################################################################################
    # The required functionality: read orders from 'orders.csv' and write trades to 'trades.csv' #
    ###############################################################################################
    orders_file = order_book_path/"orders.csv"
    trades_file = order_book_path/"trades.csv"

    get_orders_from_file_command_line = f"-t {trades_file} -f {orders_file}"

    # execute(p.parse_args(get_orders_from_file_command_line.split()))

    # Uncomment below to run through the provided small order files producing output in trades_tryN.csv

    # for n in range(1, 5):
    #     orders_file = order_book_path/f"orders{n}.csv"
    #     trades_file = order_book_path/f"trades{n}.csv"
    #     execute(p.parse_args(f"-t {trades_file} -f {orders_file}".split()))


if __name__ == '__main__':
    # For the required functionality, make sure the module 'sortedcontainers' is installed with
    #    pip3 install sortedcontainers
    # Create an appropriate orders.csv, then run
    #    python3 order_book.py -f orders.csv -t trades.csv

    parser = construct_arg_parser()
    execute(parser.parse_args())
