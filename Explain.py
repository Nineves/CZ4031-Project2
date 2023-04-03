import sqlparse
import deepdiff
from Parsers import QEP
from database_connection import DBConnection

def query_comparison(query1, query2, keyword = None):
    comparison_result = ""
    query_dict1 = parse_SQL(query1)
    query_dict2 = parse_SQL(query2)
    #keyword = "WHERE"
    ddiff = deepdiff.DeepDiff(query_dict1, query_dict2)["values_changed"]
    new_key = "root['{}']".format(keyword)
    new_value = ddiff[new_key]['new_value']
    old_value = ddiff[new_key]['old_value']
    if keyword == "WHERE":
        comparison_result += "the condition in WHERE clause has changed from [{}] to [{}]".format(old_value, new_value)
    elif keyword == "FROM":
        comparison_result += "Relations involved in P1: [{}] -> Relations involved in P2[{}]".format(old_value, new_value)
    elif keyword == "GROUP BY":
        comparison_result += "Group keys of P1:[{}] -> Group keys of P2:[{}]".format(old_value, new_value)
    elif keyword == "ORDER BY":
        comparison_result += "Sort keys of P1:[{}] -> Sort keys of P2:[{}]".format(old_value, new_value)
    
    return comparison_result


def plan_comparison(plan1, plan2, query1, query2):
    explanation_dict = {}

    plan1_relations = plan1.head_node.get_relation_names()
    plan2_relations = plan2.head_node.get_relation_names()

    plan1_nodes = plan1.all_nodes
    plan2_nodes = plan2.all_nodes

    plan1_scan_nodes = plan1.scan_nodes
    plan2_scan_nodes = plan2.scan_nodes

    plan1_join_nodes = plan1.join_nodes
    plan2_join_nodes = plan2.join_nodes

    plan1_other_nodes = set(plan1_nodes) - set(plan1_scan_nodes) - set(plan1_join_nodes)
    plan2_other_nodes = set(plan2_nodes) - set(plan2_scan_nodes) - set(plan2_join_nodes)

    scan_dict = get_nodes_diff(plan1_scan_nodes, plan2_scan_nodes)
    join_dict = get_nodes_diff(plan1_join_nodes, plan2_join_nodes)

    #Handling scan node comparisons
    for i in range(len(scan_dict['common']['P1'])):
        n1 = scan_dict['common']['P1'][i]
        n2 = scan_dict['common']['P2'][i]

        if n1.node_type != n2.node_type:
            exp = ""
            exp += explain_scan_diff(n1, n2, query1, query2)
            explanation_dict[(n1.node_number,n2.node_number)] = exp
    
    P1_only = set()
    P2_only = set()

    for item in scan_dict['only_1']:
        exp = "The scan operation {} on table [{}] only exist in Plan1.".format(item.node_type, item.relation_name)
        exp += "The reason for the change can be that the tables involved in two plans are different."
        exp += query_comparison(query1, query2, "FROM")
        explanation_dict[(item.node_number, 0)] = exp

    for item in scan_dict['only_2']:
        P2_only.add(item.relation_name)
        exp = "The scan operation {} on table [{}] only exist in Plan1.".format(item.node_type, item.relation_name)
        exp += "The reason for the change can be that the tables involved in two plans are different."
        exp += query_comparison(query1, query2, "FROM")
        explanation_dict[(0, item.node_number)] = exp


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
            explanation_dict[(n1.node_number, n2.node_number)] = exp
        elif n1.node_type == n2.node_type and relations_1 != relations_2:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The order of join operations on tables has changed from {} to {}.".format(r1, r2)
            exp += "The reason for this change is likely that {}".format(reason_new)
            explanation_dict[(n1.node_number, n2.node_number)] = exp
    
    for item in join_dict['only_1']:
        isDiffTable = False
        involved_relations = item.get_relation_names()
        for r in involved_relations:
            if r not in plan2_relations:
                isDiffTable = True
                exp = "The join operation which involves relation {} only appears in Plan1 as table {} is not used in Query2. ".format(involved_relations,r)
                exp += "The change is indicated in FROM clause: {}".format(query_comparison(query1, query2, 'FROM'))
                explanation_dict[(item.node_number, 0)] = exp
        if isDiffTable == False:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The join operation which involves relation {} only appears in Plan1".format(involved_relations)
            exp += "The reason for this change is likely that {}".format(reason_new)
            exp += "One or some of the relations in [{}] might be joined with other relations first as it is more efficient.".format(involved_relations)
            explanation_dict[(item.node_number, 0)] = exp

    for item in join_dict['only_2']:
        isDiffTable = False
        involved_relations = item.get_relation_names()
        for r in involved_relations:
            if r not in plan1_relations:
                exp = "The join operation which involves relation {} only appears in Plan2 as table {} is not used in Query1. ".format(r,r)
                exp += "The change is indicated in FROM clause: {}".format(query_comparison(query1, query2, 'FROM'))
                explanation_dict[(0, item.node_number)] = exp  
        if isDiffTable == False:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The join operation which involves relation {} only appears in Plan1".format(involved_relations)
            exp += "The reason for this change is likely that {}".format(reason_new)
            exp += "One or some of the relations in [{}] might be joined with other relations first as it is more efficient.".format(involved_relations)  
            explanation_dict[(0, item.node_number)] = exp  
    
    # Handling other nodes' differences
    others_diff_exp = get_other_nodes_diff_exp(plan1_other_nodes, plan2_other_nodes, query1, query2)
    explanation_dict.update(others_diff_exp)

    return explanation_dict

def get_other_nodes_diff_exp(nodes1, nodes2, query1, query2):
    explanation_dict = {}

    aggre_nodes1 = get_nodes(nodes1, "Aggregate")
    aggre_nodes2 = get_nodes(nodes2, "Aggregate")

    sort_nodes1 = get_nodes(nodes1, "Sort")
    sort_nodes2 = get_nodes(nodes2, "Sort")

    aggre_dict={'only_1':[], 'common':{'P1':[], 'P2':[]}, 'only_2':[]}
    for node1 in aggre_nodes1:
        for node2 in aggre_nodes2:
            if set(node1.group_key) == set(node2.group_key):
                aggre_dict['common']['P1'].append(node1)
                aggre_dict['common']['P2'].append(node2)
    aggre_dict["only_1"] = get_list_diff(aggre_nodes1,aggre_dict["common"]["P1"])
    aggre_dict["only_2"] = get_list_diff(aggre_nodes2,aggre_dict["common"]["P2"])

    for item in aggre_dict["only_1"]:
        exp = "The aggregation operation with group key {} only exists in Plan1.".format(item.group_key)
        exp += "The reason for it is that {}".format(query_comparison(query1, query2, 'GROUP BY'))
        explanation_dict[(item.node_number, 0)] = exp

    for item in aggre_dict["only_2"]:
        exp = "The aggregation operation with group key {} only exists in Plan2.".format(item.group_key)
        exp += "The reason for it is that {}".format(query_comparison(query1, query2, 'GROUP BY'))
        explanation_dict[(0, item.node_number)] = exp


    sort_dict={'only_1':[], 'common':{'P1':[], 'P2':[]}, 'only_2':[]}
    for node1 in sort_nodes1:
        for node2 in sort_nodes2:
            #print("Key1:{}, Key2:{}".format(node1.sort_key, node2.sort_key))
            if set(node1.sort_key) == set(node2.sort_key):
                sort_dict['common']['P1'].append(node1)
                sort_dict['common']['P2'].append(node2)
    sort_dict["only_1"] = get_list_diff(sort_nodes1,sort_dict["common"]["P1"])
    sort_dict["only_2"] = get_list_diff(sort_nodes2,sort_dict["common"]["P2"])

    for item in sort_dict["only_1"]:
        exp = "The sort operation with sort key {} only exists in Plan1.".format(item.sort_key)
        exp += "The reason for it is that {}".format(query_comparison(query1, query2, 'ORDER BY'))
        explanation_dict[(item.node_number, 0)] = exp

    for item in sort_dict["only_2"]:
        exp = "The sort operation with sort key {} only exists in Plan2.".format(item.sort_key)
        exp += "The reason for it is that {}".format(query_comparison(query1, query2, 'ORDER BY'))
        explanation_dict[(0, item.node_number)] = exp
    
    return explanation_dict




def get_nodes(nodes, keyword = None):
    result = []
    if keyword == None:
        return nodes
    elif keyword == "Aggregate":
        for node in nodes:
            if "Aggregate" in node.node_type:
                result.append(node)

    elif keyword == "Sort":
        for node in nodes:
            if "Sort" in node.node_type:
                result.append(node)

    return result


def get_nodes_diff(nodes1, nodes2):
    scan_dict={'only_1':[], 'common':{'P1':[], 'P2':[]}, 'only_2':[]}
    for node1 in nodes1:
        for node2 in nodes2:
            if compare_nodes(node1, node2):
                scan_dict['common']['P1'].append(node1)
                scan_dict['common']['P2'].append(node2)
    scan_dict["only_1"] = get_list_diff(nodes1,scan_dict["common"]["P1"])
    scan_dict["only_1"] = get_list_diff(nodes2,scan_dict["common"]["P2"])
    return scan_dict



def compare_nodes(node1, node2):
    result = False
    if 'Scan' in node1.node_type:
        if node1.relation_name == node2.relation_name:
            result = True
    elif 'Nested Loop' in node1.node_type or 'Join' in node2.node_type:
        if set(node1.get_relation_names()) == set(node2.get_relation_names()):
            result = True
    elif 'Join' in node1.node_type or 'Nested Loop' in node2.node_type:
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

def get_list_diff(list1, list2):
    result = []
    for item in list1:
        if item not in list2:
            result.append(item)
    return result

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



def preprocess_Tokens(tokens):
    for i, token in enumerate(tokens):
        if token.is_whitespace:
            tokens.pop(i)
    return tokens


if __name__ == "__main__":
    # FOR TESTING ONLY
    # Need to parse subquery (if there's any)
    connection = DBConnection()
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    query1 = 'select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10 order by customer.c_custkey desc;'
    query2 = 'select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_custkey >= 75000 and nation.n_nationkey >= 10 order by nation.n_name;'
    #query3 = "select o_orderpriority,count(*) as order_count from orders where o_totalprice > 100 and exists (select * from lineitem where l_orderkey = o_orderkey and l_extendedprice > 100) group by o_orderpriority order by o_orderpriority;"


    # ddiff = deepdiff.DeepDiff(parse_SQL(query1),parse_SQL(query2))["values_changed"]
    # print(ddiff)
    # exit()

    plan1 = connection.execute(FORE_WORD + query1)[0][0][0]
    plan2 = connection.execute(FORE_WORD + query2)[0][0][0]

    print(plan2)

    qep1 = QEP(QEP.parse_json_file(plan1))
    qep2 = QEP(QEP.parse_json_file(plan2))

    

    result = plan_comparison(qep1, qep2, query1, query2)
    print(result)
