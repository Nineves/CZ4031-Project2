ddiff = deepdiff.DeepDiff(parse_SQL(query1),parse_SQL(query2))["values_changed"]
    # print(ddiff)
    # exit()