from pymongo import MongoClient
import csv
import re
from gpm_logger import GPMLogger
from copy import deepcopy
from wekan_card_reader import WekanCardReader


class TradeCardReader(WekanCardReader):
    def __init__(self, board_slug, mh, mp, mdb):
        self.trades = []
        self.locations = {}
        self.logger = GPMLogger("card_reader").get_logger()
        self.pattern = re.compile("(?P<client>\d+)\s-\s(?P<otype>.*)")
        self.raw_trades = []
        self.trades = []
        self.xform_trades = []
        self.csv_card_mapping = {
            "Trade Date": "A Trade Date",
            "Delivery Date": "B Delivery Date",
            "Client Number": "Client",
            "Document No": "M Document No.",
            "Location": "E Location",
            "Transaction Type": "C Transaction Type",
            "Metal": "L Metal",
            "Order Type": "D Order Type",
            "Order Detail": "H Bars Composition",
            "Premium (%)": "G Transaction Premium (%)",
            "Referral": "I Referral",
            "Pricing Method": "F Pricing Option",
            "Remarks+": "J Remarks",
            "Labels": "labelIds",
            "Things To Do": "Things To Do",
            "Operations Team Action": "Operations Team Action",
            "BD/CR Action Needed": "BD/CR Action Needed",
            "Completed Trades": "Completed Trades",
        }
        self.trade_obj = {k: "" for k in self.csv_card_mapping.keys()}
        WekanCardReader.__init__(self, board_slug, mh, mp, mdb)

    def get_trade_object(self):
        trade = {k: "" for k in self.csv_card_mapping.keys()}
        return trade

    def load_raw_trades(self):
        cards = self.db["cards"].find({"boardId": self.board_id, "archived": False})
        for card in cards:
            try:
                raw_trade = deepcopy(self.raw_object)
                match_result = self.pattern.match(card["title"])
                if match_result:
                    raw_trade["id"] = card["_id"]
                    for field in card["customFields"]:
                        try:
                            raw_trade[field["_id"]] = field["value"]
                        except KeyError as ke:
                            raw_trade[field["_id"]] = ""
                    try:
                        raw_trade["labelIds"] = card["labelIds"]
                    except KeyError as ke:
                        raw_trade["labelIds"] = []

                    matched_dict = match_result.groupdict()
                    raw_trade["Client"] = matched_dict["client"]

                    self.raw_trades.append(raw_trade)
            except KeyError as ke:
                self.logger.debug(ex)

            except Exception as ex:
                self.logger.debug(ex)
        no_of_raw_trade = len(self.raw_trades)
        self.logger.info("Total {} raw trades found".format(no_of_raw_trade))

    def transform_raw_trade(self, trade):
        """
        level 0 : Value of the key is actual valie
        level 1 : Value of the key maps to other dictrionary. One more lookup
        Assign values to fields based on if its level 0/1 custom field
        """
        xtrade = deepcopy(trade)
        l1cfkeys = list(self.l1_customfields.keys())
        for fid, fval in trade.items():
            try:
                if fid in l1cfkeys:
                    xtrade[self.customfields[fid]] = self.l1_customfields[fid][fval]
                else:
                    xtrade[self.customfields[fid]] = fval
            except KeyError as ke:
                # self.logger.debug(ke)
                pass
            except Exception as ex:
                self.logger.debug(ex)

        for blk, blv in self.mapped_moved_cards[xtrade["id"]].items():
            xtrade[self.board_lists[blk]] = blv.strftime("%d/%m/%Y")
        return xtrade

    def transform_raw_trades(self):
        self.xform_trades = [
            self.transform_raw_trade(trade) for trade in self.raw_trades
        ]

    def load_trades(self):
        cards = self.db["cards"].find({"boardId": self.board_id})
        trades = []
        for raw_trade in self.xform_trades:
            try:
                trade = deepcopy(self.trade_obj)
                for (csv_field, card_field) in self.csv_card_mapping.items():
                    try:
                        trade[csv_field] = raw_trade[card_field]
                    except KeyError as ke:
                        trade[csv_field] = ""
                        pass
                trade["Trade Date"] = (
                    trade["Trade Date"].strftime("%d/%m/%Y")
                    if trade["Trade Date"]
                    else ""
                )
                trade["Delivery Date"] = (
                    trade["Delivery Date"].strftime("%d/%m/%Y")
                    if trade["Delivery Date"]
                    else ""
                )
                card_labels = [self.labels[lid] for lid in raw_trade["labelIds"]]
                csv_labels = " | "
                csv_labels = csv_labels.join(card_labels)
                trade["Labels"] = csv_labels
                # Replace level 2 mapping
                trades.append(trade)
            except KeyError as ke:
                self.logger.debug(ex)

            except Exception as ex:
                self.logger.debug(ex)
        no_of_qurated_trade = len(trades)
        self.logger.info("Total {} qurated trades found".format(no_of_qurated_trade))
        self.trades = trades

    def export_trades(self, ofile):
        keys = self.csv_card_mapping.keys()
        with open(ofile, "w") as trades_csv:
            trades_csv_writer = csv.DictWriter(trades_csv, keys)
            trades_csv_writer.writeheader()
            trades_csv_writer.writerows(self.trades)
        self.logger.info("Trade Status exported to file {fn}".format(fn=ofile))
