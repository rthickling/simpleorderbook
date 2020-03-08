# simpleorderbook
An Order Book for a simple stock exchange.  A simulator is also provided.

# Install
1. Install [Python 3](https://wiki.python.org/moin/BeginnersGuide/Download)
2. Use [`pip`](https://pypi.org/project/pip/) to install [`sortedcontainers`](http://www.grantjenks.com/docs/sortedcontainers/). 

    `pip install sortedcontainers`

# Run
There are two running modes:

1. Orders in a .csv file.  
   The simulator can be used to generate order files
2. Orders streamed from a [Named Pipe](https://docs.microsoft.com/en-us/windows/win32/ipc/named-pipes) (Windows) or [FIFO](http://man7.org/linux/man-pages/man7/fifo.7.html) (other systems)

## From CSV File
`python order_book.py -f <orders file> -t <trades file>` 

The list of matches is output in 'trades file'

To generate orders from the simulator.

`python order_simulator.py -g <generated orders file>`

## From the Simulator
1. Run the simulator with

    `python order_simulator.py -p <pipe name>`

2. Run the order book:

    `python order_book.py -p <pipe name> -t <trades file>`

# Help
Further details of features can be seen by running: 

`python order_book.py --help`

and

`python order_simulator.py --help`

Also, an example command line is provided in `order_simulator.py`

# Examples
1. Generate some order data: 

    `python order_simulator.py -g generated_orders.csv`

2. Using the supplied `test_orders.csv`:

    `python order_book.py -f test_orders.csv -t test_output.csv`

3. Stream orders from generator to order book:
    
    In one terminal:

    `python order_simulator.py -p order_pipe`

    and in another:

    `python order_book.py -p order_pipe -t streamed_output.csv`

# Tests
To run the tests execute:

`python order_book_test.py`

# Acknowledgements
This was written by Richard Hickling and is available to be copied.  Attribution is appreciated.
