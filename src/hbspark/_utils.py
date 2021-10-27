from pyspark.sql import Row

#Create hbase row from "columnfamiliy:column" : "value"
def hbase_row(dictionary):
    new_row = Row(**dictionary)
    return new_row

# Conversion from SO: https://stackoverflow.com/questions/33137741/fastest-way-to-convert-a-dicts-keys-values-from-bytes-to-str-in-python3
def bindict_to_strdict(data):
    if isinstance(data, bytes):  return data.decode('utf-8')
    if isinstance(data, dict):   return dict(map(bindict_to_strdict, data.items()))
    if isinstance(data, tuple):  return map(bindict_to_strdict, data)
    return data

def binlist_to_strlist(data):
    # print(type(data))
    if isinstance(data, bytes): return data.decode('utf-8')
    if isinstance(data, list): return list(map(binlist_to_strlist, data))
    if isinstance(data, tuple): return map(binlist_to_strlist, data)
    return data