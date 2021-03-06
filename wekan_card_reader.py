from pymongo import MongoClient
import csv
import re
from gpm_logger import GPMLogger
from copy import deepcopy


class WekanCardReader:
    def __init__(self, board_slug, mh, mp, mdb):
        self.mongo_host = mh
        self.mongo_port = mp
        self.db_name = mdb
        self.client = MongoClient(self.mongo_host, self.mongo_port)
        self.db = self.client[self.db_name]
        self.customfields = {}
        self.l1_customfields = {}
        board_id = self.db["boards"].find_one({"slug": board_slug})["_id"]
        self.board_id = board_id
        self.lists = None
        self.labels = None
        self.raw_object = None
        self.logger = GPMLogger(board_slug).get_logger()

    def load_custom_fields(self):
        """
        Get Level 1 custom fields
        """
        customfields = self.db.customFields
        all_fields = list(
            customfields.find(
                {"boardIds": self.board_id}, {"_id": 1, "name": 1, "type": 1}
            )
        )
        self.customfields = {field["_id"]: field["name"] for field in all_fields}
        for cfield in all_fields:
            if cfield["type"] == "dropdown":
                l1cf = self.db.customFields.find_one({"_id": cfield["_id"]})
                l1cfri = l1cf["settings"]["dropdownItems"]
                l1cfmi = self.__map_id_val(l1cfri, "name")
                self.l1_customfields[cfield["_id"]] = l1cfmi

    def __map_id_val(self, data, value_key):
        mapped_data = {}
        for field in data:
            try:
                mapped_data[field["_id"]] = field[value_key]
            except KeyError as ke:
                self.logger.debug("FAILED - {msg}".format(msg=ke))
        return mapped_data

    def __load_dropdown_items(self, field_name):
        field_items = {}
        customfields = self.db.customFields
        dropdown_items = customfields.find_one(
            {"name": field_name}, {"settings.dropdownItems": 1}
        )
        field_items = dropdown_items["settings"]["dropdownItems"]
        return field_items

    def load_labels(self):
        """
        Load Labels for a Board
        """
        field_name = "Labels"
        no_of_items = -1
        try:
            board = self.db.boards.find_one({"_id": self.board_id})
            raw_items = board["labels"]
            self.labels = self.__map_id_val(raw_items, "name")
            no_of_items = len(self.labels)
            if no_of_items > 0:
                self.logger.success(
                    "Loading {fn} : SUCCESS - {cnt}  {fn} loaded".format(
                        fn=field_name, cnt=no_of_items
                    )
                )
            elif no_of_items == 0:
                self.logger.info(
                    "Loading {fn} : SUCCESS - No {fn}. {cnt} {fn}".format(
                        fn=field_name, cnt=no_of_items
                    )
                )
            else:
                self.logger.info(
                    "Loading {fn} : FAILED - Could not load {fn}".format(fn=field_name)
                )
        except KeyError as ke:
            self.logger.debug(
                "Loading {fn} : FAILED - {msg}".format(fn=field_name, msg=ke)
            )
        except Exception as ex:
            self.logger.debug(
                "Loading {fn} : FAILED - {msg}".format(fn=field_name, msg=ex)
            )
        return no_of_items

    def load_lists(self):
        """
        Load Lists in the board
        """
        field_name = "Board Lists"
        no_of_items = -1
        try:
            raw_items = self.db.lists.find({"boardId": self.board_id})
            self.board_lists = self.__map_id_val(raw_items, "title")
            no_of_items = len(self.board_lists)
            if no_of_items > 0:
                self.logger.success(
                    "Loading {fn} : SUCCESS - {cnt}  {fn} loaded".format(
                        fn=field_name, cnt=no_of_items
                    )
                )
            elif no_of_items == 0:
                self.logger.success(
                    "Loading {fn} : SUCCESS - No {fn}. {cnt} {fn}".format(
                        fn=field_name, cnt=no_of_items
                    )
                )
            else:
                self.logger.info(
                    "Loading {fn} : FAILED - Could not load {fn}".format(fn=field_name)
                )
        except KeyError as ke:
            self.logger.debug(
                "Loading {fn} : FAILED - {msg}".format(fn=field_name, msg=ke)
            )
        except Exception as ex:
            self.logger.debug(
                "Loading {fn} : FAILED - {msg}".format(fn=field_name, msg=ex)
            )
        return no_of_items

    def load_card_activities(self):
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
                self.mapped_moved_cards[card["cardId"]][card["listId"]] = card[
                    "modifiedAt"
                ]
            except KeyError as ke:
                if card["cardId"] == ke.args[0]:
                    self.mapped_moved_cards[card["cardId"]] = {}
                    self.mapped_moved_cards[card["cardId"]][card["listId"]] = card[
                        "modifiedAt"
                    ]
                else:
                    pass
            """
            try:
                self.mapped_moved_cards[card["cardId"]][
                    self.board_lists[card["listId"]]
                ] = card["modifiedAt"]
            except KeyError as ke:
                self.mapped_moved_cards[card["cardId"]] = {}
                self.mapped_moved_cards[card["cardId"]][
                    self.board_lists[card["listId"]]
                ] = card["modifiedAt"]
            """

    def load_cards(self):
        self.load_custom_fields()
        self.load_lists()
        self.load_labels()
        self.load_card_activities()
        self.__build_raw_object()

    def __build_raw_object(self):
        raw_object = {}
        for field_id in self.customfields.keys():
            raw_object[field_id] = ""
        # for field_id in self.l1_customfields.keys():
        #     raw_object[field_id] = ""
        for field_id in self.board_lists.keys():
            raw_object[field_id] = ""
        self.raw_object = raw_object
