from pymongo import MongoClient
import csv


class TradeCardReader():
    def __init__(self):
        self.mongo_host = 'localhost'
        self.mongo_port= 27019
        self.db_name = 'wekan'
        self.client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.client[self.db_name]
        self.customfields = {}
        self.board_id = None
        self.trades = []
        self.locations = {}
        self.fieldnames = ["A Trade Date", "E Location", "C Transaction Type", "D Order Type", "H Bars Composition", "G Transaction Premium (%)", "I Referral", "F Pricing Option", "J Remarks", "B Delivery Date"]
        self.csv_header = ["Trade Date", " Location", " Transaction Type", " Order Type", " Order Detail", " Premium (%)", " Referral", " Pricing Method", " Remarks", "Delivery Date"]
    

    def load_level1_custom_fields(self):
        """
        Get Level 1 custom fields
        """
        customfields = self.db.customFields
        all_fields = list(customfields.find({},{"_id":1, "name":1}))
        self.customfields = { field["_id"]: field["name"] for field in all_fields }
        print (self.customfields)

    def load_locations(self):
        """
        Load Locations
        """
        customfields = self.db.customFields
        location_items = customfields.find_one({"name":"E Location"},{"settings.dropdownItems":1})
        raw_locations = location_items["settings"]["dropdownItems"]
        for raw_location in raw_locations:
            self.locations[raw_location["_id"]] = raw_location["name"]

        print (self.locations)


    def get_trades_for_board(self,board_slug):
        board_id = self.db['boards'].find_one({"slug":board_slug})["_id"]
        self.board_id = board_id
        cards = self.db['cards'].find({"boardId":self.board_id})
        trades = []
        for card in cards:
            trade = {}
            for field in card['customFields']:                
                try:
                    trade[self.customfields[field["_id"]]] = field["value"]
                except KeyError as ke:
                    trade[self.customfields[field["_id"]]] = ""
            trades.append(trade)
        self.trades = trades[:1]

    def export_trades(self, ofile):
        keys = self.trades[0].keys()
        print(keys)
        with open(ofile, 'w') as trades_csv:
            trades_csv_writer = csv.DictWriter(trades_csv,self.fieldnames)
            trades_csv_writer.writeheader()
            trades_csv_writer.writerows(self.trades)
        print ("Trades exported to CSV in file {}".format(ofile))


if __name__ ==  "__main__":
    tcr = TradeCardReader()
    tcr.load_level1_custom_fields()
    tcr.get_trades_for_board("gpm-trades")
    tcr.load_locations()
    # tcr.export_trades('trades_sample_4.csv')
