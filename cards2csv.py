#!/usr/bin/env python

import argparse
import os
from datetime import datetime
from gpm_trade_cards_reader import TradeCardReader
from gpm_logger import GPMLogger

if __name__ == "__main__":
    logger = GPMLogger("report_generator").get_logger()

    ofile = "{}-{}.csv".format(
        "Trade-Status-Report", datetime.now().strftime("%d-%b-%Y")
    )

    parser = argparse.ArgumentParser(description="Generate csv for trade cards")
    parser.add_argument(
        "-b",
        "--board",
        default="gpm-trades-2020",
        help="Board ID of the board to generate CSV for",
    )
    parser.add_argument(
        "-cf", "--conf_file", help="Config file containing configuration"
    )
    parser.add_argument("-od", "--odir", help="Directory to store the generated csv to")
    parser.add_argument("-o", "--out", help="File name to export to", default=ofile)
    parser.add_argument("-mh", "--mhost", help="MongoDB host", default="localhost")
    parser.add_argument("-mp", "--mport", help="MongoDB port", default=27019)
    parser.add_argument("-d", "--db", help="MongoDB database", default="wekan")

    args = parser.parse_args()
    board_slug = args.board
    mongo_host = args.mhost
    mongo_port = args.mport
    mongo_db = args.db
    odir = args.odir
    ofile = args.out

    logger.info("Generating  : {}".format(ofile))

    tcr = TradeCardReader(board_slug, mongo_host, mongo_port, mongo_db)
    tcr.load_lists()
    tcr.load_level1_custom_fields()
    tcr.load_locations()
    tcr.load_transaction_type()
    tcr.load_order_type()
    tcr.load_pricing_options()
    tcr.load_metal_types()
    tcr.load_labels()
    tcr.get_moved_cards()
    tcr.get_trades_for_board()

    tcr.export_trades(ofile)
