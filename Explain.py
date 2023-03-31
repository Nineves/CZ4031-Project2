import sqlparse
import deepdiff
import Parsers


def query_comparison(query1, query2, keyword = None):
    comparison_result = ""
    query_dict1 = parse_SQL(query1)
    query_dict2 = parse_SQL(query2)
    #keyword = "WHERE"
    ddiff = deepdiff.DeepDiff(query_dict1, query_dict2)["values_changed"]
    new_key = "root['{}']".format(keyword)
    new_value = ddiff['new_value']
    old_value = ddiff['old_value']
    if keyword == "WHERE":
        comparison_result += "the condition in WHERE clause has changed from ['{}'] to ['{}']".format(old_value, new_value)
    elif keyword == "FROM":
        comparison_result += "Relations involved in P1: [{}] -> Relations involved in P2[{}]".format(old_value, new_value)
    
    return comparison_result


def plan_comparison(plan1, plan2, query1, query2):
    explanation_list = []

    plan1_relations = plan1.get_relation_names()
    plan2_relations = plan2.get_relation_names()

    plan1_nodes = plan1.all_nodes
    plan2_nodes = plan2.all_nodes

    plan1_scan_nodes = plan1.scan_nodes
    plan2_scan_nodes = plan2.scan_nodes

    plan1_join_nodes = plan1.join_nodes
    plan2_join_nodes = plan2.join_nodes

    scan_dict = get_nodes_diff(plan1_scan_nodes, plan2_scan_nodes)
    join_dict = get_nodes_diff(plan1_join_nodes, plan2_join_nodes)

    #Handling scan node comparisons
    for i in range(len(scan_dict['common']['P1'])):
        n1 = scan_dict['common']['P1'][i]
        n2 = scan_dict['common']['P2'][i]

        if n1.node_type != n2.node_type:
            exp = ""
            exp += explain_scan_diff(n1, n2, query1, query2)
            explanation_list.append(exp)
    
    P1_only = set()
    P2_only = set()

    for item in scan_dict['only_1']:
        P1_only.add(item.relation_name)

    for item in scan_dict['only_2']:
        P2_only.add(item.relation_name)

    if len(P1_only)!=0:
        s1 = ', '.join(P1_only)
    else:
        s1 = ''

    if len(P2_only)!=0:
        s2 = ', '.join(P2_only)
    else:
        s2 = ''

    exp = "The scan operation on tables [{}] only exist in Plan1. The scan operation on tables [{}] only exist in Plan2.".format(s1,s2)
    exp += "The reason for the change can be that the tables involved in two plans are different."
    exp += query_comparison(query1, query2, "FROM")
    explanation_list.append(exp)


    #Handling join node comparisons
    for i in range(len(join_dict['common']['P1'])):
        n1 = join_dict['common']['P1'][i]
        n2 = join_dict['common']['P2'][i]
        relations_1 = n1.get_relation_names()
        relations_2 = n2.get_relation_names()
        r1 = ', '.join(relations_1)
        r2 = ', '.join(relations_2)


        if n1.node_type != n2.node_type:
            exp = ""
            exp += explain_join_diff(n1, n2, query1, query2)
            explanation_list.append(exp)
        elif n1.node_type == n2.node_type and relations_1 != relations_2:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The order of join operations on tables has changed from {} to {}.".format(r1, r2)
            exp += "The reason for this change is likely that {}".format(reason_new)
    
    for item in join_dict['only_1']:
        isDiffTable = False
        involved_relations = item.get_relation_names()
        for r in involved_relations:
            if r not in plan2_relations:
                isDiffTable = True
                exp = "The join operation which involves relation {} only appears in Plan1 as table {} is not used in Query2. ".format(involved_relations,r)
                exp += "The change is indicated in FROM clause: {}".format(query_comparison(query1, query2, 'FROM'))
        if isDiffTable == False:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The join operation which involves relation {} only appears in Plan1".format(involved_relations)
            exp += "The reason for this change is likely that {}".format(reason_new)
            exp += "One or some of the relations in [{}] might be joined with other relations first as it is more efficient.".format(involved_relations)

    for item in join_dict['only_2']:
        isDiffTable = False
        involved_relations = item.get_relation_names()
        for r in involved_relations:
            if r not in plan1_relations:
                exp = "The join operation which involves relation {} only appears in Plan2 as table {} is not used in Query1. ".format(r,r)
                exp += "The change is indicated in FROM clause: {}".format(query_comparison(query1, query2, 'FROM'))  
        if isDiffTable == False:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The join operation which involves relation {} only appears in Plan1".format(involved_relations)
            exp += "The reason for this change is likely that {}".format(reason_new)
            exp += "One or some of the relations in [{}] might be joined with other relations first as it is more efficient.".format(involved_relations)  
    return scan_dict, join_dict

def get_nodes_diff(nodes1, nodes2):
    scan_dict={'only_1':[], 'common':{'P1':[], 'P2':[]}, 'only_2':[]}
    for node1 in nodes1:
        for node2 in nodes2:
            if compare_nodes(node1, node2):
                scan_dict['common']['P1'].append(node1)
                scan_dict['common']['P2'].append(node2)
    scan_dict["only_1"] = node1 - scan_dict["common"]["P1"]
    scan_dict["only_2"] = node1 - scan_dict["common"]["P2"]
    return scan_dict



def compare_nodes(node1, node2):
    result = False
    if 'Scan' in node1.node_type:
        if node1.relation_name == node2.relation_name:
            result = True
    elif 'Nested Loop' in node1.node_type or 'Join' in node2.node_type:
        if set(node1.get_relation_names()) == set(node2.get_relation_names()):
            result = True
    
    return result


def parse_SQL(query):

    SQL_dict = dict()
    parsed = sqlparse.parse(sqlparse.format(query, keyword_case='upper'))[0]
    tokens = parsed.tokens
    # for t in tokens:
    #     print(t.value)
    # return
    updated_tokens = preprocess_Tokens(tokens)

    for i, token in enumerate(updated_tokens):
        if token.value == 'SELECT':
            SQL_dict['SELECT'] = tokens[i + 1].value
            continue

        if 'WHERE' in token.value:
            tmp = token.value[6:]  # delete "WHERE " from the value
            SQL_dict['WHERE'] = tmp  # parse for the "AND"
            continue

        if token.is_keyword:
            SQL_dict[token.value] = tokens[i + 1].value
            continue
    return SQL_dict

def explain_scan_diff(node1, node2, query1, query2):
    explanation = ""

    if node1.node_type == "Seq Scan" and node2.node_type == "Index Scan":
        explanation += "Sequential scan on table {} has evolved to Index scan.".format(node1.relation_name)
        if node1.table_filter != node2.index_cond:
            explanation += "The reason for the change can be that selection condition has changed from {} to {}.".format(node1.table_filter, node2.index_cond)
    elif node1.node_type == "Index Scan" and node2.node_type == "Seq Scan":
        explanation += "Index scan on table {} has evolved to Sequential scan.".format(node1.relation_name)
        if node1.table_filter != node2.index_cond:
            explanation += "The reason for the change can be that selection condition has changed from {} to {}.".format(node1.index_cond, node2.table_filter)  
    
    return explanation

def explain_join_diff(node1, node2, query1, query2):
    explanation = ""

    relations_1 = node1.get_relation_names()
    relations_2 = node2.get_relation_names()
    r1 = ', '.join(relations_1)
    r2 = ', '.join(relations_2)

    explanation += "Join operation which involves tables {} has evloved from {} to {}. ".format(r1, node1.node_type, node2.node_type)
    if relations_1 != relations_2:
        explanation += "And the sequence of join operations on tables has also changed from {} to {}.".format(r1, r2)

    reason_new = query_comparison(query1, query2, 'WHERE')
    explanation += "The reason for this change is likely that {}".format(reason_new)
    return explanation


    # explanation += "The join operation has evolved from {} on relation {} to {} on relation {} .".format(node1.node_type, r1, node2.node_type, r2)
    # if relations_1 != relations_2:
    #     if len(relations_1) != len(relations_2):
    #         isTableDiff = True
    #         reason = "the number of tables involved in P1 and the number of tables involved in P2 are different."
    #     else:
    #         if set(relations_1) != set(relations_2):
    #             isTableDiff = True
    #             reason = "the tables involved in P1 and the tables involved in P2 are different."
    #         elif set(relations_1) == set(relations_2):
    #             reason = "the sequence of tables involved in P1 and the sequence of tables involved in P2 are different."
        
    # if isTableDiff == False:
    #     reason_new = query_comparison(query1, query2, 'WHERE')
    #     reason += "and " + reason_new

    # explanation += "The reason for this change is probably that " + reason


def preprocess_Tokens(tokens):
    for i, token in enumerate(tokens):
        if token.is_whitespace:
            tokens.pop(i)
    return tokens


if __name__ == "__main__":
    # Need to parse subquery (if there's any)
    query1 = 'SELECT customer.c_custkey, customer.c_name, nation.n_name from customer, nation WHERE customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10'
    query2 = 'SELECT customer.c_custkey, customer.c_name, nation.n_name from customer, nation WHERE customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000'
    # sql_dict = parse_SQL(query1)
    # print(sql_dict)
    query_comparison(query1, query2)
