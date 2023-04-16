import sqlparse
import deepdiff
import json
from Parsers import QEP
from database_connection import DBConnection

def get_QEP_description(query, connection):
    '''
    Returns natural language description of a QEP.
    '''
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    plan = connection.execute(FORE_WORD + query)[0][0][0]
    parsed_plan = QEP.parse_json_file(plan)
    current_QEP = QEP(parsed_plan)
    return current_QEP.generate_NL_description()

def plot_tree_graph(query1, query2, connection):
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    plan_1 = connection.execute(FORE_WORD + query1)[0][0][0]
    parsed_plan_1 = QEP.parse_json_file(plan_1)
    current_QEP_1 = QEP(parsed_plan_1)

    plan_2 = connection.execute(FORE_WORD + query2)[0][0][0]
    parsed_plan_2 = QEP.parse_json_file(plan_2)
    current_QEP_2 = QEP(parsed_plan_2)

    explanation_dict = plan_comparison(current_QEP_1, current_QEP_2, query1, query2)
    diff_indexs_1 = get_diff_node_index(explanation_dict, 1)
    diff_indexs_2 = get_diff_node_index(explanation_dict, 2)

    current_QEP_1.plot(diff_indexs_1, "1")
    current_QEP_2.plot(diff_indexs_2, "2")



def query_comparison(query1, query2, keyword = None):
    '''
    Returns a dictionary which records the differences between two queries.
    '''
    values_changed = None
    values_removed = None
    values_added = None
    comparison_result = ""
    query_dict1 = parse_SQL(query1)
    query_dict2 = parse_SQL(query2)
    #keyword = "WHERE"
    new_key = "root['{}']".format(keyword)
    ddiff = deepdiff.DeepDiff(query_dict1, query_dict2)

    if "values_changed" in ddiff.keys() and new_key in ddiff["values_changed"]:
        values_changed = ddiff["values_changed"]
        new_value = values_changed[new_key]['new_value']
        old_value = values_changed[new_key]['old_value']
        if keyword == "WHERE":
            comparison_result += "the condition in WHERE clause has changed from [{}] to [{}]. ".format(old_value, new_value)
        elif keyword == "FROM":
            comparison_result += "Relations involved in query 1: [{}] -> Relations involved in query 2: [{}]. ".format(old_value, new_value)
        elif keyword == "GROUP BY":
            comparison_result += "Group keys of query 1:[{}] -> Group keys of query 2: [{}]. ".format(old_value, new_value)
        elif keyword == "ORDER BY":
            comparison_result += "Sort keys of query 1:[{}] -> Sort keys of query 2: [{}]. ".format(old_value, new_value)

    if "dictionary_item_removed" in ddiff.keys():
        values_removed = ddiff["dictionary_item_removed"]
        if new_key in values_removed:
            comparison_result += "Clause [{}] has been removed from query 1. ".format(keyword)

    if "dictionary_item_added" in ddiff.keys():
        values_added = ddiff["dictionary_item_added"]
        if new_key in values_added:
            comparison_result += "Clause [{}] has been added to query 2. ".format(keyword)
    return comparison_result

def get_advantage(N1, N2):
    '''
    Explains why a node type is better than the other type in current QEP.
    '''

    advantage = ""
    if N1.node_type == "Index Scan" and N2.node_type == "Seq Scan":
        advantage += " Index scan can be used only when search condition contains attributes with index. "
    elif N1.node_type == "Seq Scan" and N2.node_type == "Index Scan":
        advantage += " Index scan is often faster than sequential scan, since index access significantly reduces the number of I/O read operations. "
    elif N1.node_type == "Nested Loop" and N2.node_type == "Hash Join":
        advantage += " Hash Join is more suitable for equi-join, where relations not sorted and no indexes exist. "
    elif N1.node_type == "Hash Join" and N2.node_type == "Nested Loop":
        advantage += " HNested loop is useful when the left argument has a small size (fewer outer loops). "
    elif N1.node_type == "Hash Join" and N2.node_type == "Merge Join":
        advantage += " Tables involved in the join operation of QEP 2 can be sorted on {} effectively. And merge join is more suitable for non-equi join. ".format(N2.merge_cond)
    elif N1.node_type == "Merge Join" and N2.node_type == "Hash Join":
        advantage += " Hash Join is more suitable for equi-join, where relations not sorted and no indexes exist. "
    elif N1.node_type == "Merge Join" and N2.node_type == "Nested Loop":
        advantage += " Nested loop is useful when the left argument has a small size (fewer outer loops). "
    elif N1.node_type == "Nested Loop" and N2.node_type == "Merge Join":
        advantage += " Tables involved in the join operation of QEP 2 can be sorted on {} effectively. And merge join is more suitable for non-equi join. ".format(N2.merge_cond)
    
    return advantage

def diff_explanation_in_NL(query1, query2, connection):
    '''
    Returns differences between two QEPs and explanation for the differences in natural languages.
    '''

    FORE_WORDS = "explain (analyze, costs, verbose, buffers, format json) "
    plan1 = connection.execute(FORE_WORDS + query1)[0][0][0]
    plan2 = connection.execute(FORE_WORDS + query2)[0][0][0]

    qep1 = QEP(QEP.parse_json_file(plan1))
    qep2 = QEP(QEP.parse_json_file(plan2))

    result = plan_comparison(qep1, qep2, query1, query2)

    explanation = ""

    for key in result.keys():
        if key[0] == 0:
            explanation += "Node {} in QEP 2 does not exist in QEP 1: {}\n\n".format(str(key[1]), result[key])
        elif key[1] == 0:
            explanation += "Node {} in QEP 1 does not exist in QEP 2: {}\n\n".format(str(key[0]), result[key])
        else:
            explanation += "Node {} in QEP 1 is different from node {} in QEP 2: {}\n\n".format(str(key[0]), str(key[1]), result[key])
    if explanation == "":
        explanation += "No major difference has been found between QEP 1 and QEP 2. "

    return explanation


def plan_comparison(plan1, plan2, query1, query2):

    '''
    This function return a comparison result in a dictionary form.
    E.g.
    (1,2):".......", means node 1 in QEP 1 is different from node 2 in QEP 2.
    (0,3):".....", means node 3 only exists in QEP 2.
    
    '''
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
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp += "The reason for this change is likely that {} ".format(reason_new)
            advantage = get_advantage(n1,n2)
            exp += advantage
            
            explanation_dict[(n1.node_number,n2.node_number)] = exp
        elif n1.table_filter !=n2.table_filter:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The filter condition has changed from {} to {}. ".format(n1.table_filter, n2.table_filter)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            explanation_dict[(n1.node_number, n2.node_number)] = exp
        elif n1.index_cond != n2.index_cond:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The index condition has changed from {} to {}. ".format(n1.index_cond, n2.index_cond)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            explanation_dict[(n1.node_number, n2.node_number)] = exp
    
    P1_only = set()
    P2_only = set()

    for item in scan_dict['only_1']:
        exp = "The scan operation {} on table [{}] only exist in Plan1. ".format(item.node_type, item.relation_name)
        exp += "The reason for the change can be that the tables involved in two plans are different. "
        exp += query_comparison(query1, query2, "FROM")
        explanation_dict[(item.node_number, 0)] = exp

    for item in scan_dict['only_2']:
        P2_only.add(item.relation_name)
        exp = "The scan operation {} on table [{}] only exist in Plan1. ".format(item.node_type, item.relation_name)
        exp += "The reason for the change can be that the tables involved in two plans are different. "
        exp += query_comparison(query1, query2, "FROM")
        explanation_dict[(0, item.node_number)] = exp


    #Handling join node comparisons
    for i in range(len(join_dict['common']['P1'])):
        n1 = join_dict['common']['P1'][i]
        n2 = join_dict['common']['P2'][i]
        relations_1 = n1.get_relation_names()
        relations_2 = n2.get_relation_names()
        r1 = ' x '.join(relations_1)
        r2 = ' x '.join(relations_2)


        if n1.node_type != n2.node_type:
            exp = ""
            exp += explain_join_diff(n1, n2, query1, query2)
            explanation_dict[(n1.node_number, n2.node_number)] = exp
        elif n1.node_type == n2.node_type and relations_1 != relations_2:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The order of join operations on tables in subtrees of the node has changed from {} to {}. ".format(r1, r2)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            advantage = get_advantage(n1,n2)
            exp += advantage
            explanation_dict[(n1.node_number, n2.node_number)] = exp

        elif n1.node_type == n2.node_type and n1.merge_cond != n2.merge_cond:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The merge condition has changed from {} to {}. ".format(n1.merge_cond, n2.merge_cond)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            explanation_dict[(n1.node_number, n2.node_number)] = exp
            
        elif n1.node_type == n2.node_type and n1.hash_cond != n2.hash_cond:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The hash condition has changed from {} to {}. ".format(n1.hash_cond, n2.hash_cond)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            explanation_dict[(n1.node_number, n2.node_number)] = exp
        
        
    for item in join_dict['only_1']:
        isDiffTable = False
        involved_relations = item.get_relation_names()
        for r in involved_relations:
            if r not in plan2_relations:
                isDiffTable = True
                exp = "The join operation which involves relations {} only appears in Plan1 as table {} is not used in Query2. ".format(involved_relations,r)
                exp += "The change is indicated in FROM clause: {} ".format(query_comparison(query1, query2, 'FROM'))
                explanation_dict[(item.node_number, 0)] = exp
        if isDiffTable == False:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The join operation which involves relation {} only appears in Plan1. ".format(involved_relations)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            exp += "One or some of the relations in [{}] might be joined with other relations first as it is more efficient. ".format(involved_relations)
            explanation_dict[(item.node_number, 0)] = exp

    for item in join_dict['only_2']:
        isDiffTable = False
        involved_relations = item.get_relation_names()
        for r in involved_relations:
            if r not in plan1_relations:
                exp = "The join operation which involves relation {} only appears in Plan2 as table {} is not used in Query1. ".format(r,r)
                exp += "The change is indicated in FROM clause: {} ".format(query_comparison(query1, query2, 'FROM'))
                explanation_dict[(0, item.node_number)] = exp  
                
        if isDiffTable == False:
            reason_new = query_comparison(query1, query2, 'WHERE')
            exp = "The join operation which involves relation {} only appears in Plan1. ".format(involved_relations)
            exp += "The reason for this change is likely that {} ".format(reason_new)
            exp += "One or some of the relations in [{}] might be joined with other relations first as it is more efficient. ".format(involved_relations)  
            explanation_dict[(0, item.node_number)] = exp  
    
    # Handling other nodes' differences
    others_diff_exp = get_other_nodes_diff_exp(plan1_other_nodes, plan2_other_nodes, query1, query2)
    explanation_dict.update(others_diff_exp)

    return explanation_dict

def get_diff_node_index(explanation_dict, QEP_number):

    '''
    This function returns a list evolved node indexes for a QEP.
    '''

    index_list = []
    if QEP_number == 1:
        for key in explanation_dict.keys():
            if key[0] != 0:
                index_list.append(key[0])
    elif QEP_number == 2:
        for key in explanation_dict.keys():
            if key[1] != 0:
                index_list.append(key[1])
    return index_list



def get_other_nodes_diff_exp(nodes1, nodes2, query1, query2):
    explanation_dict = {}

    aggre_nodes1 = get_nodes(nodes1, "Aggregate")
    aggre_nodes2 = get_nodes(nodes2, "Aggregate")

    sort_nodes1 = get_nodes(nodes1, "Sort")
    sort_nodes2 = get_nodes(nodes2, "Sort")

    
    #Handling aggregate node comparisons
    aggre_dict={'only_1':[], 'common':{'P1':[], 'P2':[]}, 'only_2':[]}
    for node1 in aggre_nodes1:
        for node2 in aggre_nodes2:
            if set(node1.group_key) == set(node2.group_key):
                aggre_dict['common']['P1'].append(node1)
                aggre_dict['common']['P2'].append(node2)
    aggre_dict["only_1"] = get_list_diff(aggre_nodes1,aggre_dict["common"]["P1"])
    aggre_dict["only_2"] = get_list_diff(aggre_nodes2,aggre_dict["common"]["P2"])
    

    for item in aggre_dict["only_1"]:
        exp = "The aggregation operation with group key {} only exists in Plan1. ".format(item.group_key)
        if query_comparison(query1, query2, 'GROUP BY') != "":
            exp += "The reason for it is that {}".format(query_comparison(query1, query2, 'GROUP BY'))
        else:
            exp += "The reason for it is that clause GROUP BY {} only exists in query 1.\n".format(item.group_key)
        explanation_dict[(item.node_number, 0)] = exp

    for item in aggre_dict["only_2"]:
        exp = "The aggregation operation with group key {} only exists in Plan2. ".format(item.group_key)

        if query_comparison(query1, query2, 'GROUP BY') != "":
            exp += "The reason for it is that {}".format(query_comparison(query1, query2, 'GROUP BY'))
        else:
            exp += "The reason for it is that clause GROUP BY {} only exists in query 2.\n".format(item.group_key)
        explanation_dict[(0, item.node_number)] = exp

    #Handling sort node comparisons
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
        exp = "The sort operation with sort key {} only exists in Plan1. ".format(item.sort_key)
        ans = query_comparison(query1, query2, 'ORDER BY')
        if query_comparison(query1, query2, 'ORDER BY') == "":
            ans = query_comparison(query1, query2, 'WHERE')
        exp += "The reason for it is that {}".format(ans)
        explanation_dict[(item.node_number, 0)] = exp

    for item in sort_dict["only_2"]:
        exp = "The sort operation with sort key {} only exists in Plan2. ".format(item.sort_key)
        ans = query_comparison(query1, query2, 'ORDER BY')
        if query_comparison(query1, query2, 'ORDER BY') == "":
            ans = query_comparison(query1, query2, 'WHERE')
        exp += "The reason for it is that {}".format(ans)
        explanation_dict[(0, item.node_number)] = exp
    
    return explanation_dict


def get_nodes(nodes, keyword = None):
    '''
    Returns a list of nodes of certain node type (specified by keyword).
    '''
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
    '''
    Returns True if the two nodes are deemed to be put in the 'common' sub dictionary.
    '''
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
    elif 'Sort' in node1.node_type:
        if set(node1.get_relation_names()) == set(node2.get_relation_names()):
            result = True
    elif 'Aggregate' in node1.node_type:
        if set(node1.get_relation_names()) == set(node2.get_relation_names()):
            result = True
    
    return result


def parse_SQL(query):

    '''
    Parse SQL query into a dictionary form.
    '''

    SQL_dict = dict()
    parsed = sqlparse.parse(sqlparse.format(query, keyword_case='upper'))[0]
    tokens = parsed.tokens
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
        explanation += "Sequential scan on table {} has evolved to Index scan. ".format(node1.relation_name)
        if node1.table_filter != node2.index_cond:
            explanation += "The selection condition has changed from {} to {}. ".format(node1.table_filter, node2.index_cond)
    elif node1.node_type == "Index Scan" and node2.node_type == "Seq Scan":
        explanation += "Index scan on table {} has evolved to Sequential scan.".format(node1.relation_name)
        if node1.table_filter != node2.index_cond:
            explanation += "The selection condition has changed from {} to {}. ".format(node1.index_cond, node2.table_filter)  
    
    return explanation

def explain_join_diff(node1, node2, query1, query2):
    explanation = ""

    relations_1 = node1.get_relation_names()
    relations_2 = node2.get_relation_names()
    r1 = ', '.join(relations_1)
    r2 = ', '.join(relations_2)

    explanation += "Join operation which involves tables {} has evloved from {} to {}. ".format(r1, node1.node_type, node2.node_type)
    if relations_1 != relations_2:
        explanation += "And the sequence of join operations on tables has also changed from {} to {}. ".format(r1, r2)

    reason_new = query_comparison(query1, query2, 'WHERE')
    explanation += "The reason for this change is likely that {} ".format(reason_new)

    return explanation


def preprocess_Tokens(tokens):
    for i, token in enumerate(tokens):
        if token.is_whitespace:
            tokens.pop(i)
    return tokens

def doExperiment1(connection):
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    query1 = 'select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10 order by customer.c_custkey desc;'
    query2 = 'select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_custkey >= 75000 and nation.n_nationkey >= 10 order by nation.n_name;'

    plan1 = connection.execute(FORE_WORD + query1)[0][0][0]
    plan2 = connection.execute(FORE_WORD + query2)[0][0][0]

    with open('Exp_qep1_1.json', 'w',newline='\r\n') as f:
        json.dump(plan1, f, indent=2)
    with open('Exp_qep1_2.json', 'w',newline='\r\n') as f:
        json.dump(plan2, f, indent=2)

    result = diff_explanation_in_NL(query1, query2, connection)
    with open('Explanation_1.txt', 'w',newline='\r\n') as f:
        f.write(result)


def doExperiment2(connection):
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    query1 = "select supplier.s_name, supplier.s_acctbal from nation, supplier, lineitem where nation.n_name = 'JAPAN' and supplier.s_nationkey = nation.n_nationkey and lineitem.l_suppkey = supplier.s_suppkey and lineitem.l_quantity = 30;"
    query2 = "select supplier.s_name, supplier.s_acctbal from nation, supplier, lineitem where nation.n_name != 'JAPAN' and supplier.s_nationkey = nation.n_nationkey and lineitem.l_suppkey = supplier.s_suppkey and lineitem.l_quantity = 30;"

    plan1 = connection.execute(FORE_WORD + query1)[0][0][0]
    plan2 = connection.execute(FORE_WORD + query2)[0][0][0]

    with open('Exp_qep2_1.json', 'w',newline='\r\n') as f:
        json.dump(plan1, f, indent=2)
    with open('Exp_qep2_2.json', 'w',newline='\r\n') as f:
        json.dump(plan2, f, indent=2)

    result = diff_explanation_in_NL(query1, query2, connection)
    with open('Explanation_2.txt', 'w',newline='\r\n') as f:
        f.write(result)

def doExperiment3(connection):
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    query1 = "select * from (SELECT supplier.s_nationkey,supplier.s_suppkey FROM supplier WHERE 20=s_suppkey) AS a join (SELECT nation.n_nationkey, nation.n_regionkey FROM nation) As b on a.s_nationkey = b.n_nationkey"
    query2 = "select * from (SELECT supplier.s_nationkey,supplier.s_suppkey FROM supplier WHERE 200<=s_suppkey) AS a join (SELECT nation.n_nationkey, nation.n_regionkey FROM nation) As b on a.s_nationkey = b.n_nationkey"

    plan1 = connection.execute(FORE_WORD + query1)[0][0][0]
    plan2 = connection.execute(FORE_WORD + query2)[0][0][0]

    with open('Exp_qep3_1.json', 'w',newline='\r\n') as f:
        json.dump(plan1, f, indent=2)
    with open('Exp_qep3_2.json', 'w',newline='\r\n') as f:
        json.dump(plan2, f, indent=2)

    result = diff_explanation_in_NL(query1, query2, connection)
    with open('Explanation_3.txt', 'w',newline='\r\n') as f:
        f.write(result)

def doExperiment4(connection):
    query1 = "select customer.c_custkey, customer.c_name, nation_a.n_name from customer, (select n_nationkey, n_name from nation where n_nationkey >= 10) as nation_a where customer.c_nationkey = nation_a.n_nationkey and customer.c_acctbal >= 1000 order by customer.c_custkey desc;"
    query2 = "select customer_a.c_custkey, customer_a.c_name, nation.n_name from (select c_name, c_custkey, c_nationkey from customer where customer.c_custkey <= 750) as customer_a, nation where customer_a.c_nationkey = nation.n_nationkey and nation.n_nationkey >= 10 order by customer_a.c_custkey desc;"
    result = diff_explanation_in_NL(query1, query2, connection)
    with open('Explanation_4.txt', 'w',newline='\r\n') as f:
        f.write(result)

def doExperiment5(connection):
    FORE_WORD = "explain (analyze, costs, verbose, buffers, format json) "
    query1 = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_totalprice > 10 and l_extendedprice > 10 group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate;"
    query2 = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_totalprice > 10 group by l_orderkey, o_orderdate, o_shippriority;"
    
    plan1 = connection.execute(FORE_WORD + query1)[0][0][0]
    plan2 = connection.execute(FORE_WORD + query2)[0][0][0]

    with open('Exp_qep5_1.json', 'w',newline='\r\n') as f:
        json.dump(plan1, f, indent=2)
    with open('Exp_qep5_2.json', 'w',newline='\r\n') as f:
        json.dump(plan2, f, indent=2)
    
    
    result = diff_explanation_in_NL(query1, query2, connection)
    with open('Explanation_5.txt', 'w',newline='\r\n') as f:
        f.write(result)


if __name__ == "__main__":
    ########################################## FOR TESTING ONLY################################################
    # Need to parse subquery (if there's any)
    connection = DBConnection()
    #doExperiment1(connection)
    #doExperiment2(connection)
    #doExperiment3(connection)
    #doExperiment4(connection)
    #doExperiment5(connection)
    #query1 = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_totalprice > 10 and l_extendedprice > 10 group by l_orderkey, o_orderdate, o_shippriority order by revenue desc, o_orderdate;"
    #r1= parse_SQL(query1)
    #query2 = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_totalprice > 10 group by l_orderkey, o_orderdate, o_shippriority;"
    #r2 = parse_SQL(query2)
    #r3 = query_comparison(query1, query2, "ORDER BY")
    #q = "explain (analyze, costs, verbose, buffers, format json) select customer.c_custkey from customer where customer.c_custkey >= 75000;"
    #print(connection.execute(q)[0][0][0])


    
