from compliance_card_reader import ComplianceCardReader

if __name__ == "__main__":
    board_slug = "compliance-work"
    mongo_host = "localhost"
    mongo_port = 27019
    mongo_db = "wekan"

    tcr = ComplianceCardReader(board_slug, mongo_host, mongo_port, mongo_db)
    tcr.load_cards()
    tcr.load_raw_trades()
    tcr.transform_raw_trades()
    # print(tcr.raw_trades[:5])
    # print (wcr.raw_object)
    # tcr.xform_trades[0]
    tcr.load_trades()
    tcr.export_trades("cmpl1.csv")
