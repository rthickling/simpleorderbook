# simpleorderbook
An Order Book for a simple stock exchange

# Install
1. Install [Python 3](https://wiki.python.org/moin/BeginnersGuide/Download)
2. Use `pip` to install `sortedcontainers`. 

    `pip install sortedcontainers`

# Run
There are two running modes:

1. Orders in a .csv file.  
   The simulator can be used to generate order files
2. Orders streamed from a Named Pipe (Windows) or FIFO (other systems)

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