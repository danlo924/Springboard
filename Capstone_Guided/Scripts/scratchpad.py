import json
# file = "C:/Temp/data/json/2020-08-05/NASDAQ/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.txt"
# f = open(file)
j = '{"trade_dt":"2020-08-05","file_tm":"2020-08-05 09:30:00.000","event_type":"Q","symbol":"SYMA","event_tm":"2020-08-05 09:36:55.284","event_seq_nb":1,"exchange":"NASDAQ","bid_pr":76.10016521142818,"bid_size":100,"ask_pr":77.9647975908747,"ask_size":100}'
data = json.loads(j)
print(data["event_type"])


# def parse_json(line:str):
#     record = json.loads(line)
#     record_type = record["event_type"]
#     try:
#         # [logic to parse records]
#         if record_type == "T":
#             event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"], record["symbol"], record["exchange"], 
#                 datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'), int(record["event_seq_nb"]), datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), 
#                 Decimal(record["price"]), int(record["size"]), None, None, None, None, "T", None)
#         elif record_type == "Q":
#             event = common_event(datetime.strptime(record["trade_dt"], "%Y-%m-%d"), record["event_type"], record["symbol"], record["exchange"], 
#                 datetime.strptime(record["event_tm"], '%Y-%m-%d %H:%M:%S.%f'), int(record["event_seq_nb"]), datetime.strptime(record["file_tm"], '%Y-%m-%d %H:%M:%S.%f'), 
#                 None, None, Decimal(record["bid_pr"]), int(record["bid_size"]), Decimal(record["ask_pr"]), int(record["ask_size"]), "Q", None)
#             return event
#     except Exception as e:
#         return common_event(None, None, None, None, None, None, None, None, None, None, None, None, None, "B", line)