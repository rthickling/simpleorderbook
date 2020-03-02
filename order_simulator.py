import argparse
import errno
import logging
import time
import os
import os.path
from abc import abstractmethod
from random import seed
from random import randint
import csv

import platform
if platform.system() == 'Windows':
    import win32pipe
    import win32file


class OrderPipeSender(object):
    @classmethod
    def create_order_pipe_sender(cls, pipe_name):
        if platform.system() == 'Windows':
            return WindowsOrderPipeSender(pipe_name)
        else:
            return UnixOrderPipeSender(pipe_name)

    @abstractmethod
    def send_line(self, line):
        """
        Gets a line from the pipe
        :return: returns the line
        """


class WindowsOrderPipeSender(OrderPipeSender):
    def __init__(self, pipe_name):
        self.pipe = win32pipe.CreateNamedPipe(
            r'\\.\pipe\\' + pipe_name,
            win32pipe.PIPE_ACCESS_DUPLEX,
            win32pipe.PIPE_TYPE_MESSAGE | win32pipe.PIPE_READMODE_MESSAGE | win32pipe.PIPE_WAIT,
            1, 0x10000, 0x10000, 0, None)
        logging.info("Waiting for client")
        win32pipe.ConnectNamedPipe(self.pipe, None)
        logging.info("Connected to client")

    def __del__(self):
        logging.info("Disconnecting pipe")
        win32pipe.DisconnectNamedPipe(self.pipe)
        logging.info("Closing file handle")
        win32file.CloseHandle(self.pipe)

    def send_line(self, line):
        order_data = str.encode(f"{line}")
        win32file.WriteFile(self.pipe, order_data)


class UnixOrderPipeSender(OrderPipeSender):
    def __init__(self, pipe_name):
        pipe_path = f"./{pipe_name}"
        self.pipe = None
        if not os.path.exists(pipe_path):
            os.mkfifo(pipe_path)
        while True:
            try:
                self.pipe = os.open(pipe_path, os.O_WRONLY | os.O_NONBLOCK)
                logging.info("Pipe opened for writing")
                return
            except OSError as ex:
                if ex.errno == errno.ENXIO:
                    logging.info("waiting for client")
                    time.sleep(0.5)  # Wait 1/2 second
                    continue
                else:
                    raise

    def send_line(self, line):
        line += '\n'
        return os.write(self.pipe, line.encode())

    def __del__(self):
        logging.info("Closing pipe")
        if self.pipe:
            os.close(self.pipe)


def generate_orders_from_file(file_name):
    with open(file_name) as order_file:
        order_reader = csv.reader(order_file)
        next(order_reader, None)
        for order in order_reader:
            yield ','.join(order)


def random_order_generator(delay=0, number_of_orders=0, size_range=None, price_range=None, random_seed=None,
                           names=None, clients=None):
    """Create a sequence of valid orders, within bounds, for provided names and clients"""
    seed(random_seed)
    sides = ['Buy', 'Sell']
    min_size, max_size = size_range
    min_price, max_price = price_range
    check_orders = (number_of_orders > 0)
    while True:
        if delay:
            time.sleep(delay)
        if check_orders:
            number_of_orders -= 1
            if number_of_orders < 0:
                break
        yield (f"{clients[randint(0, len(clients)-1)]},{names[randint(0, len(names)-1)]},"
               f"{sides[randint(0, 1)]},"
               f"{10*randint(min_size/10, max_size/10)},{min_price + randint(0, max_price - min_price)}")


def generate_orders(delay, pipe_name, number_of_orders=None, orders_file=None, **kwargs):
    """Gets orders either randomized or from a file and sends them via a pipe"""
    if orders_file:
        order_generator = generate_orders_from_file
        order_kwargs = {'file_name': orders_file}
    else:
        order_generator = random_order_generator
        order_kwargs = dict(delay=delay, number_of_orders=number_of_orders, **kwargs)

    order_pipe = OrderPipeSender.create_order_pipe_sender(pipe_name)

    for order in order_generator(**order_kwargs):
        logging.debug(f"Writing order {order}")
        order_pipe.send_line(f"{order}")

    logging.info("Finished")


def generate_order_file(file_name, **kwargs):
    """Uses the random_order_generator to create a new order file"""
    header = ['Customer', 'Item', 'Side', 'Quantity', 'Price']
    with open(file_name, 'w+') as order_file:
        order_writer = csv.writer(order_file, lineterminator='\r')
        order_writer.writerow(header)
        for order in random_order_generator(**kwargs):
            order_writer.writerow(order.split(','))


def execute(arguments):
    if arguments.debug:
        logging.basicConfig(level=logging.DEBUG)
    if arguments.generate_file:
        generate_order_file(arguments.generate_file, number_of_orders=arguments.number_of_orders, delay=arguments.delay,
                            random_seed=arguments.random_seed, size_range=arguments.size_range,
                            price_range=arguments.price_range, names=arguments.names, clients=arguments.clients)
    elif arguments.orders_file:
        generate_orders(arguments.delay, arguments.pipe_name, orders_file=arguments.orders_file)
    else:
        generate_orders(arguments.delay, arguments.pipe_name, arguments.number_of_orders,
                        random_seed=arguments.random_seed,
                        size_range=arguments.size_range, price_range=arguments.price_range,
                        names=arguments.names, clients=arguments.clients)


def construct_arg_parser():
    p = argparse.ArgumentParser(description="Simulator for stock orders")
    p.add_argument('-p', '--pipe_name', required=False, default='order_pipe', help='Name of the named pipe')
    group = p.add_mutually_exclusive_group()
    group.add_argument('-f', '--orders_file', required=False, help='path to a file of orders to send')
    group.add_argument('-g', '--generate_file', required=False, help='path of the generated orders')
    p.add_argument('-n', '--number_of_orders', type=int, required=False, default=100,
                   help='number of orders to generate')
    p.add_argument('-d', '--delay', type=float, metavar='seconds', required=False, default=0.0,
                   help='number of seconds (decimal) between orders')
    p.add_argument('-D', '--debug', action='store_true', help='turn on DEBUG logging')
    p.add_argument('-r', '--random_seed', metavar='seed', type=int, default=1, help='random number seed')
    p.add_argument('-S', '--size_range', type=int, nargs=2, metavar='size', default=[10, 200],
                   help='range of allowed sizes')
    p.add_argument('-P', '--price_range', type=int, nargs=2, metavar='price', default=[90, 130],
                   help='range of allowed prices')
    p.add_argument('-N', '--names', nargs='*', default=['IBM', 'AMZN'], help='names for which to generate orders')
    p.add_argument('-C', '--clients', nargs='*', default=['Jane', 'Bob', 'Chris', 'Mark', 'Phillip'],
                   help='clients for which to generate orders')
    return p


def main():
    p = construct_arg_parser()
    execute(p.parse_args(
        "-g/home/richard/projects/OrderBook/orders_large_test.csv"
        " -D -n1000 -d0 -r2 -N AAPL MSFT AMZN FB GOOG JPM GOOGL JNJ WMT V PG BRK.B BAC XOM MA T DIS UNH INTC "
        "VZ -C Anne Bob Colin Dom Elliot Fred Greg Henry Ian John Kevin Lyle Mike Nigel Oliver Peter Quentin "
        "Richard Steve Tony Ulysees Victor Walter Xavier Yves Zachery"
        " -p 'order_pipe' -S 10 200 -P 50 150 -g ".split()))


if __name__ == '__main__':
    parser = construct_arg_parser()
    execute(parser.parse_args())
