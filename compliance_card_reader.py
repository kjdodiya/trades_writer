from pymongo import MongoClient
import csv
import re
from gpm_logger import GPMLogger
from copy import deepcopy
from wekan_card_reader import WekanCardReader


class ComplianceCardReader(WekanCardReader):
    def __init__(self, board_slug, mh, mp, mdb):
        self.trades = []
        self.locations = {}
        self.logger = GPMLogger("compliance_reader").get_logger()
        self.pattern = re.compile("(?P<client>\d+)\s(?P<detail>.*)")
        self.raw_trades = []
        self.trades = []
        self.xform_trades = []
        self.csv_card_mapping = {
            "Title": "title",
            "Created": "createdAt",
            "Labels": "labelIds",
            "For Review": "For Review",
            "For Client Contact": "For Client Contact",
            "For Compliance Officer Clearing": "For Compliance Officer Clearing",
            "Compliance Officer Cleared": "Compliance Officer Cleared",
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

                    raw_trade["createdAt"] = card["createdAt"]
                    raw_trade["title"] = card["title"]
                    matched_dict = match_result.groupdict()
                    # raw_trade["Client"] = matched_dict["clieint"]
                    # raw_trade["Detail"] = matched_dict["detail"]
                    self.raw_trades.append(raw_trade)
            except KeyError as ke:
                self.logger.debug(ke)

            except Exception as ex:
                self.logger.debug(ex)
        no_of_raw_trade = len(self.raw_trades)
        self.logger.info(
            "Total {} raw compliance records found".format(no_of_raw_trade)
        )

    def transform_raw_trade(self, trade):
        """
        level 0 : Value of the key is actual valie
        level 1 : Value of the key maps to other dictrionary. One more lookup
        Assign values to fields based on if its level 0/1 custom field
        """
        xtrade = deepcopy(trade)
        l1cfkeys = list(self.l1_customfields.keys())
        for fid, fval in trade.items():
            if self.l1_customfields:
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
        xtrade["createdAt"] = trade["createdAt"].strftime("%d/%m/%Y")
        try:
            for blk, blv in self.mapped_moved_cards[xtrade["id"]].items():
                try:
                    xtrade[self.board_lists[blk]] = blv.strftime("%d/%m/%Y")
                except KeyError as ke:
                    pass
        except KeyError as ke:
            pass
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
                trade = self.get_trade_object()
                for (csv_field, card_field) in self.csv_card_mapping.items():
                    try:
                        trade[csv_field] = raw_trade[card_field]
                    except KeyError as ke:
                        trade[csv_field] = ""
                        pass
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
        self.logger.info(
            "Total {} compliance cards qurated".format(no_of_qurated_trade)
        )
        self.trades = trades

    def export_trades(self, ofile):
        keys = self.csv_card_mapping.keys()
        with open(ofile, "w") as trades_csv:
            trades_csv_writer = csv.DictWriter(trades_csv, keys)
            trades_csv_writer.writeheader()
            trades_csv_writer.writerows(self.trades)
        self.logger.info("Compliance Status exported to file {fn}".format(fn=ofile))
