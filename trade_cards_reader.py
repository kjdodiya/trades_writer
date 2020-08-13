from pymongo import MongoClient
import csv
import re


class TradeCardReader:
    def __init__(self, board_slug):
        self.mongo_host = "localhost"
        self.mongo_port = 27019
        self.db_name = "wekan"
        self.client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.client[self.db_name]
        self.customfields = {}
        self.trades = []
        self.locations = {}
        board_id = self.db["boards"].find_one({"slug": board_slug})["_id"]
        self.board_id = board_id
        self.pattern = re.compile("(?P<client>\d+)\s-\s(?P<otype>.*)")
        self.fieldnames = [
            "A Trade Date",
            "E Location",
            "C Transaction Type",
            "D Order Type",
            "H Bars Composition",
            "G Transaction Premium (%)",
            "I Referral",
            "F Pricing Option",
            "J Remarks",
            "B Delivery Date",
        ]
        self.csv_header = [
            "Trade Date",
            "Location",
            "Transaction Type",
            "Order Type",
            "Order Detail",
            "Premium (%)",
            "Referral",
            "Pricing Method",
            "Remarks",
            "Delivery Date",
        ]
        self.csv_card_mappping = {
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
            "Operations Team Execution": "Operations Team Execution",
            "BD/CR Action Needed": "BD/CR Action Needed",
            "Completed Trades": "Completed Trades",
        }

    def load_level1_custom_fields(self):
        """
        Get Level 1 custom fields
        """
        customfields = self.db.customFields
        all_fields = list(customfields.find({}, {"_id": 1, "name": 1}))
        self.customfields = {field["_id"]: field["name"] for field in all_fields}

    def __map_id_val(self, data, value_key):
        mapped_data = {}
        mapped_data = {field["_id"]: field[value_key] for field in data}
        return mapped_data

    def __load_dropdown_items(self, field_name):
        field_items = {}
        customfields = self.db.customFields
        dropdown_items = customfields.find_one(
            {"name": field_name}, {"settings.dropdownItems": 1}
        )
        field_items = dropdown_items["settings"]["dropdownItems"]
        return field_items

    def load_locations(self):
        """
        Load Locations
        """
        try:
            customfields = self.db.customFields
            location_items = customfields.find_one(
                {"name": "E Location"}, {"settings.dropdownItems": 1}
            )
            raw_locations = location_items["settings"]["dropdownItems"]
            self.locations = self.__map_id_val(raw_locations, "name")
            no_of_locations = len(self.locations)
            if no_of_locations > 0:
                print(
                    "Location load Successful : {} locations loaded".format(
                        len(self.locations)
                    )
                )
            elif no_of_locations == 0:
                print("Location load successful : There are no locations")
            else:
                print("Error loading locations")
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load locations")
        except Exception as ex:
            print(ex)
            print("Could not load locations")

    def load_transaction_type(self):
        """
        Load Transaction Type
        """
        field_name = "Transcation Type"
        try:
            raw_items = self.__load_dropdown_items("C Transaction Type")
            self.txn_types = self.__map_id_val(raw_items, "name")
            no_of_items = len(self.txn_types)
            if no_of_items > 0:
                print(
                    "{} load Successful : {} {} loaded".format(
                        field_name, no_of_items, field_name
                    )
                )
            elif no_of_items == 0:
                print(
                    "{} load successful : There are no {}".format(
                        field_name, field_name
                    )
                )
            else:
                print("Error loading {}".format(field_name))
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load {}".format(field_name))
        except Exception as ex:
            print(ex)
            print("Could not load {}".format(field_name))

    def load_order_type(self):
        """
        Load Order Type
        """
        field_name = "Order Type"
        try:
            raw_items = self.__load_dropdown_items("D Order Type")
            self.order_types = self.__map_id_val(raw_items, "name")
            no_of_items = len(self.order_types)
            if no_of_items > 0:
                print(
                    "{} load Successful : {} {} loaded".format(
                        field_name, no_of_items, field_name
                    )
                )
            elif no_of_items == 0:
                print(
                    "{} load successful : There are no {}".format(
                        field_name, field_name
                    )
                )
            else:
                print("Error loading {}".format(field_name))
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load {}".format(field_name))
        except Exception as ex:
            print(ex)
            print("Could not load {}".format(field_name))

    def load_pricing_options(self):
        """
        Load Pricing Options
        """
        field_name = "Pricing Option"
        try:
            raw_items = self.__load_dropdown_items("F Pricing Option")
            self.pricing_options = self.__map_id_val(raw_items, "name")
            no_of_items = len(self.pricing_options)
            if no_of_items > 0:
                print(
                    "{} load Successful : {} {} loaded".format(
                        field_name, no_of_items, field_name
                    )
                )
            elif no_of_items == 0:
                print(
                    "{} load successful : There are no {}".format(
                        field_name, field_name
                    )
                )
            else:
                print("Error loading {}".format(field_name))
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load {}".format(field_name))
        except Exception as ex:
            print(ex)
            print("Could not load {}".format(field_name))

    def load_metal_types(self):
        """
        Load Metal type
        """
        field_name = "Metal"
        try:
            raw_items = self.__load_dropdown_items("L Metal")
            self.metal_types = self.__map_id_val(raw_items, "name")
            no_of_items = len(self.metal_types)
            if no_of_items > 0:
                print(
                    "{} load Successful : {} {} loaded".format(
                        field_name, no_of_items, field_name
                    )
                )
                print(self.metal_types)
            elif no_of_items == 0:
                print(
                    "{} load successful : There are no {}".format(
                        field_name, field_name
                    )
                )
            else:
                print("Error loading {}".format(field_name))
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load {}".format(field_name))
        except Exception as ex:
            print("Error loading {}".format(field_name))

    def load_labels(self):
        """
        Load Labels for a Board
        """
        field_name = "Labels"
        try:
            board = self.db.boards.find_one({"_id": self.board_id})
            raw_items = board["labels"]
            self.labels = self.__map_id_val(raw_items, "name")
            no_of_items = len(self.labels)
            if no_of_items > 0:
                print(
                    "{} load Successful : {} {} loaded".format(
                        field_name, no_of_items, field_name
                    )
                )
            elif no_of_items == 0:
                print(
                    "{} load successful : There are no {}".format(
                        field_name, field_name
                    )
                )
            else:
                print("Error loading {}".format(field_name))
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load {}".format(field_name))
        except Exception as ex:
            print("Error loading {}".format(field_name))

    def load_lists(self):
        """
        Load Lists in the board
        """
        field_name = "Board Lists"
        try:
            raw_items = self.db.lists.find({"boardId": self.board_id})
            self.board_lists = self.__map_id_val(raw_items, "title")
            no_of_items = len(self.board_lists)
            if no_of_items > 0:
                print(
                    "{} load Successful : {} {} loaded".format(
                        field_name, no_of_items, field_name
                    )
                )
            elif no_of_items == 0:
                print(
                    "{} load successful : There are no {}".format(
                        field_name, field_name
                    )
                )
            else:
                print("Error loading {}".format(field_name))
        except KeyError as ke:
            print("One or more Key could not be found. Check MongoDB")
            print("Could not load {}".format(field_name))
        except Exception as ex:
            print("Error loading {}".format(field_name))
        print(">>> BOARD LISTS <<<<", self.board_lists)

    def get_moved_cards(self):
        query_field = {
            "$and": [
                {"boardId": self.board_id},
                {"activityType": {"$in": ["createCard", "moveCard"]}},
                {"listId": {"$in": list(self.board_lists.keys())}},
            ]
        }

        filter_fields = {
            "listId": 1,
            "listName": 1,
            "cardId": 1,
            "modifiedAt": 1,
            "createdAt": 1,
        }
        moved_cards = self.db["activities"].find(query_field, filter_fields)
        # Map Card movement to titles
        self.mapped_moved_cards = {}

        for card in moved_cards:
            try:
                if card["listId"] == "XbpDFBQhQCLgRkZar":
                    print(
                        "Processing ",
                        card["modifiedAt"],
                        card[self.board_lists["XbpDFBQhQCLgRkZar"]],
                        card["title"],
                    )

                self.mapped_moved_cards[card["cardId"]][
                    self.board_lists[card["listId"]]
                ] = card["modifiedAt"]
            except KeyError as ke:
                self.mapped_moved_cards[card["cardId"]] = {}
                self.mapped_moved_cards[card["cardId"]][
                    self.board_lists[card["listId"]]
                ] = card["modifiedAt"]

    def get_trade_object(self):
        trade = {}
        trade = {
            "Trade Date": "",
            "Delivery Date": "",
            "Client Number": "",
            "Document No": "",
            "Location": "",
            "Transaction Type": "",
            "Metal": "",
            "Order Type": "",
            "Order Detail": "",
            "Premium (%)": "",
            "Referral": "",
            "Pricing Method": "",
            "Remarks+": "",
            "Labels": "",
            "Things To Do": "",
            "Operations Team Execution": "",
            "BD/CR Action Needed": "",
            "Completed Trades": "",
        }
        return trade

    def get_trades_for_board(self):
        cards = self.db["cards"].find({"boardId": self.board_id})
        raw_trades = []
        for card in cards:
            try:
                raw_trade = {}
                match_result = self.pattern.match(card["title"])
                if match_result:
                    raw_trade["id"] = card["_id"]
                    for field in card["customFields"]:
                        try:
                            raw_trade[self.customfields[field["_id"]]] = field["value"]
                        except KeyError as ke:
                            raw_trade[self.customfields[field["_id"]]] = ""
                    try:
                        raw_trade["labelIds"] = card["labelIds"]
                    except KeyError as ke:
                        raw_trade["labelIds"] = []
                    matched_dict = match_result.groupdict()
                    raw_trade["Client"] = matched_dict["client"]
                    raw_trade["Things To Do"] = ""
                    raw_trade["Operations Team Execution"] = ""
                    raw_trade["BD/CR Action Needed"] = ""
                    raw_trade["Completed Trades"] = ""
                    raw_trades.append(raw_trade)
            except KeyError as ke:
                # print ("KeyError ", ke)
                pass
            except Exception as ex:
                # print ("Error ", ex)
                pass
        no_of_raw_trade = len(raw_trades)
        print("Total {} raw trades found".format(no_of_raw_trade))
        trades = []
        for raw_trade in raw_trades:
            try:
                trade = self.get_trade_object()
                for (csv_field, card_field) in self.csv_card_mappping.items():
                    try:
                        trade[csv_field] = raw_trade[card_field]
                    except KeyError as ke:
                        pass
                #                trade = {
                #                   csv_field: raw_trade[card_field]
                #                  for (csv_field, card_field) in self.csv_card_mappping.items()
                #                 if card_field != ""
                #            }
                # Replace level 2 mapping
                trade["Location"] = (
                    self.locations[trade["Location"]] if trade["Location"] else ""
                )
                trade["Transaction Type"] = (
                    self.txn_types[trade["Transaction Type"]]
                    if trade["Transaction Type"]
                    else ""
                )
                trade["Order Type"] = (
                    self.order_types[trade["Order Type"]] if trade["Order Type"] else ""
                )
                trade["Pricing Method"] = (
                    self.pricing_options[trade["Pricing Method"]]
                    if trade["Pricing Method"]
                    else ""
                )
                trade["Metal"] = (
                    self.metal_types[trade["Metal"]] if trade["Metal"] else ""
                )
                try:
                    card_move_lists = self.mapped_moved_cards[raw_trade["id"]]
                except KeyError as ke:
                    print(ke)
                # print (">>> {}".format(raw_trade["id"]), card_move_lists)
                try:
                    trade["Things To Do"] = card_move_lists["Things To Do"].strftime(
                        "%d/%m/%Y"
                    )
                except KeyError as ke:
                    trade["Things To Do"] = ""

                try:
                    trade["Operations Team Execution"] = card_move_lists[
                        "Operations Team Execution"
                    ].strftime("%d/%m/%Y")
                except KeyError as ke:
                    trade["Operations Team Execution"] = ""

                try:
                    trade["BD/CR Action Needed"] = card_move_lists[
                        "BD/CR Action Needed"
                    ].strftime("%d/%m/%Y")
                except KeyError as ke:
                    trade["BD/CR Action Needed"] = ""

                try:
                    trade["Completed Trades"] = card_move_lists[
                        "Completed Trades"
                    ].strftime("%d/%m/%Y")
                except KeyError as ke:
                    trade["Completed Trades"] = ""

                # Formatting
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
                card_labels = [self.labels[lid] for lid in trade["Labels"]]
                card_labels = [self.labels[lid] for lid in trade["Labels"]]
                csv_labels = " | "
                csv_labels = csv_labels.join(card_labels)
                trade["Labels"] = csv_labels
                trades.append(trade)
            except KeyError as ke:
                pass

            except Exception as ex:
                pass

        no_of_qurated_trade = len(trades)
        print("Total {} qurated trades found".format(no_of_qurated_trade))
        self.trades = trades

    def export_trades(self, ofile):
        keys = self.trades[0].keys()
        with open(ofile, "w") as trades_csv:
            trades_csv_writer = csv.DictWriter(trades_csv, keys)
            trades_csv_writer.writeheader()
            trades_csv_writer.writerows(self.trades)
        print("Trades exported to CSV in file {}".format(ofile))


if __name__ == "__main__":
    tcr = TradeCardReader("gpm-trades-2020")
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
    tcr.export_trades("gpm-trades-2020.csv")
