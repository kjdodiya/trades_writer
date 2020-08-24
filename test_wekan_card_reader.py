from wekan_card_reader import WekanCardReader

if __name__ == "__main__":
    board_slug = "gpm-trades-2020"
    mongo_host = "localhost"
    mongo_port = 27019
    mongo_db = "wekan"

    wcr = WekanCardReader(board_slug, mongo_host, mongo_port, mongo_db)
    wcr.load_cards()
    print(wcr.raw_object)
