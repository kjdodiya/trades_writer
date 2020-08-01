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

    def __map_id_val(self,data,value_key):
        mapped_data = {}
        mapped_data = { field["_id"]: field[value_key] for field in data }
        return mapped_data

    def __load_dropdown_items(self, field_name):
        field_items = {}
        customfields = self.db.customFields
        dropdown_items = customfields.find_one({"name": field_name},{"settings.dropdownItems":1})
        field_items = dropdown_items["settings"]["dropdownItems"]
        return field_items

    def load_locations(self):
        """
        Load Locations
        """
        try:            
            customfields = self.db.customFields
            location_items = customfields.find_one({"name":"E Location"},{"settings.dropdownItems":1})
            raw_locations = location_items["settings"]["dropdownItems"]
            self.locations = self.__map_id_val(raw_locations,"name")
            no_of_locations = len(self.locations)
            if no_of_locations > 0:
                print ("Location load Successful : {} locations loaded".format(len(self.locations)))
            elif no_of_locations == 0:
                print ("Location load successful : There are no locations")
            else:
                print ("Error loading locations")
        except KeyError as ke:
            print ("One or more Key could not be found. Check MongoDB")
            print ("Could not load locations")
        except Exception as ex:
            print (ex)
            print ("Could not load locations")

    def load_transaction_type(self):
        """
        Load Transaction Type
        """
        field_name = "Transcation Type"
        try:            
            raw_items = self.__load_dropdown_items("C Transaction Type")
            self.txn_types = self.__map_id_val(raw_items,"name")
            no_of_items = len(self.txn_types)
            if no_of_items > 0:
                print ("{} load Successful : {} {} loaded".format(field_name, no_of_items, field_name))
            elif no_of_items == 0:
                print ("{} load successful : There are no {}".format(field_name, field_name))
            else:
                print ("Error loading {}".format(field_name))
        except KeyError as ke:
            print ("One or more Key could not be found. Check MongoDB")
            print ("Could not load {}".format(field_name))
        except Exception as ex:
            print (ex)
            print ("Could not load {}".format(field_name))
            
    def load_order_type(self):
        """
        Load Order Type
        """
        field_name = "Order Type"
        try:            
            raw_items = self.__load_dropdown_items("D Order Type")
            self.order_types = self.__map_id_val(raw_items,"name")
            no_of_items = len(self.order_types)
            if no_of_items > 0:
                print ("{} load Successful : {} {} loaded".format(field_name, no_of_items, field_name))
            elif no_of_items == 0:
                print ("{} load successful : There are no {}".format(field_name, field_name))
            else:
                print ("Error loading {}".format(field_name))
        except KeyError as ke:
            print ("One or more Key could not be found. Check MongoDB")
            print ("Could not load {}".format(field_name))
        except Exception as ex:
            print (ex)
            print ("Could not load {}".format(field_name))


    def load_pricing_options(self):
        """
        Load Pricing Options
        """
        field_name = "Pricing Option"
        try:            
            raw_items = self.__load_dropdown_items("F Pricing Option")
            self.order_types = self.__map_id_val(raw_items,"name")
            no_of_items = len(self.order_types)
            if no_of_items > 0:
                print ("{} load Successful : {} {} loaded".format(field_name, no_of_items, field_name))
            elif no_of_items == 0:
                print ("{} load successful : There are no {}".format(field_name, field_name))
            else:
                print ("Error loading {}".format(field_name))
        except KeyError as ke:
            print ("One or more Key could not be found. Check MongoDB")
            print ("Could not load {}".format(field_name))
        except Exception as ex:
            print (ex)
            print ("Could not load {}".format(field_name))

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
    tcr.load_transaction_type()
    tcr.load_order_type()
    tcr.load_pricing_options()
    # tcr.export_trades('trades_sample_4.csv')
