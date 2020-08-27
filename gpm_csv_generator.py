#!/usr/bin/env python

import argparse
import os
from datetime import datetime
from compliance_card_reader import ComplianceCardReader
from trade_card_reader import TradeCardReader
from gpm_logger import GPMLogger

if __name__ == "__main__":
    logger = GPMLogger("report_generator").get_logger()



    parser = argparse.ArgumentParser(description="Generate csv for compliance cards")
    parser.add_argument(
        "-b",
        "--board",
        help="Board ID of the board to generate CSV for [gpm-trades-2020/compliance-work]",
    )
    parser.add_argument(
        "-cf", "--conf_file", help="Config file containing configuration"
    )
    parser.add_argument("-od", "--odir", help="Directory to store the generated csv to")
    parser.add_argument("-mh", "--mhost", help="MongoDB host", default="localhost")
    parser.add_argument("-mp", "--mport", help="MongoDB port", default=27019)
    parser.add_argument("-d", "--db", help="MongoDB database", default="wekan")
    parser.add_argument("-rt", "--report", help="Type of report (trade/compliance)", required=True)
    parser.add_argument("-o", "--out", help="File name to export to")


    args = parser.parse_args()
    board_slug = args.board
    mongo_host = args.mhost
    mongo_port = args.mport
    mongo_db = args.db
    odir = args.odir
    ofile = args.out
    report = args.report

    ofile = "{}-{}-{}.csv".format(
        report.capitalize(), "Status-Report", datetime.now().strftime("%d-%b-%Y")
        )

    logger.info("Generating  : {}".format(ofile))
    
    if report == 'trade':
        board_slug = "gpm-trades-2020"
        cr = TradeCardReader(board_slug, mongo_host, mongo_port, mongo_db)

    if report == 'compliance':
        board_slug = "compliance-work"
        cr = ComplianceCardReader(board_slug, mongo_host, mongo_port, mongo_db)

    cr.load_cards()
    cr.load_raw_trades()
    cr.transform_raw_trades()
    cr.load_trades()
    cr.export_trades(ofile)
