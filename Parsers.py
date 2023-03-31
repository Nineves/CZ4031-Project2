import queue
import json


class Node(object):
    def __init__(self, node_type, relation_name, schema, alias, group_key, sort_key, 
                 join_type, index_name, hash_cond, table_filter, index_cond, merge_cond, recheck_cond, join_filter, 
                 subplan_name, actual_rows, actual_time,description):
        self.node_type = node_type
        self.parent_node = None
        self.child_nodes = []
        self.relation_name = relation_name
        self.schema = schema
        self.alias = alias #Alternative name of a subquery
        self.group_key = group_key
        self.sort_key = sort_key
        self.join_type = join_type
        self.index_name = index_name #For index scan
        self.hash_cond = hash_cond  #For hash join
        self.table_filter = table_filter
        self.index_cond = index_cond    #For index join
        self.merge_cond = merge_cond    #For merge join
        self.recheck_cond = recheck_cond    #For bitmap scan
        self.join_filter = join_filter
        """
        E.g.
        Update needs to consider three child tables as well as the originally-mentioned parent table. 
        So there are four input scanning Subplans, one per table.
        """
        self.subplan_name = subplan_name
        self.actual_rows = actual_rows
        self.actual_time = actual_time
        self.description = description
    
    def add_child(self, child_node):
        self.child_nodes.append(child_node)
    
    def get_child_names(self):
        result = ""
        for child in self.child_nodes:
            result += child.relation_name + ", "
        if result != "":
            result = result[:,-2]
        return result

    def get_relation_names(self):
        print("Name:{}".format(self.node_type))
        result = []
        for child in self.child_nodes:
            #print("Num of children:{}".format(len(self.child_nodes)))
            #print("Child Name:{} of {}".format(child.node_type, self.node_type))
            # if 'Scan' in child.node_type:
            #     print("Scan on relation:", child.relation_name) 
            if child.relation_name != None:
                result.append(child.relation_name)
            else: 
                result += child.get_relation_names()
        return result  
    
    def get_node_info(self):
        print("Node type:{}".format(self.node_type))
        print("Involved relations:{}".format(self.get_relation_names()))
        print("Child nodes:", end=" ")
        for child in self.child_nodes:
            print(child.node_type, end=" ")
                

class QEP(object):

    def __init__(self, head_node = None):
        self.head_node = head_node
        self.scan_nodes = []
        self.join_nodes = []
        self.all_nodes = self.get_all_nodes()
    
    def get_all_nodes(self):
        if self.head_node is None:
            return []

        nodes = []
        temp = []

        temp.append(self.head_node)

        while len(temp) != 0:
            cur_node = temp.pop(0)
            if 'Scan' in cur_node.node_type:
                self.scan_nodes.append(cur_node)
            if 'Nested Loop' in cur_node.node_type or 'Join' in cur_node.node_type:
                self.join_nodes.append(cur_node)
            nodes.append(cur_node)
            if len(cur_node.child_nodes) != 0:
                for child in cur_node.child_nodes:
                    temp.append(child)
        return nodes

         
        

    @staticmethod
    def parse_json_file(json_file):
        """
        Used for parsing query plan to query plan tree
        """
        plan_to_parse = json_file['Plan']
        plans = queue.Queue()
        nodes = queue.Queue()
        parent_node = None

        plans.put(plan_to_parse)

        while not plans.empty():
            cur_plan = plans.get()
            if not nodes.empty():
                parent_node = nodes.get()

            #Initialize current node
            relation_name = schema = alias = group_key = sort_key = join_type = index_name = hash_cond = table_filter \
            = index_cond = merge_cond = recheck_cond = join_filter = subplan_name = actual_rows = actual_time = description = None
            if 'Relation Name' in cur_plan:
                relation_name = cur_plan['Relation Name']
            if 'Schema' in cur_plan:
                schema = cur_plan['Schema']
            if 'Alias' in cur_plan:
                alias = cur_plan['Alias']
            if 'Group Key' in cur_plan:
                group_key = cur_plan['Group Key']
            if 'Sort Key' in cur_plan:
                sort_key = cur_plan['Sort Key']
            if 'Join Type' in cur_plan:
                join_type = cur_plan['Join Type']
            if 'Index Name' in cur_plan:
                index_name = cur_plan['Index Name']
            if 'Hash Cond' in cur_plan:
                hash_cond = cur_plan['Hash Cond']
            if 'Filter' in cur_plan:
                table_filter = cur_plan['Filter']
            if 'Index Cond' in cur_plan:
                index_cond = cur_plan['Index Cond']
            if 'Merge Cond' in cur_plan:
                merge_cond = cur_plan['Merge Cond']
            if 'Recheck Cond' in cur_plan:
                recheck_cond = cur_plan['Recheck Cond']
            if 'Join Filter' in cur_plan:
                join_filter = cur_plan['Join Filter']
            if 'Actual Rows' in cur_plan:
                actual_rows = cur_plan['Actual Rows']
            if 'Actual Total Time' in cur_plan:
                actual_time = cur_plan['Actual Total Time']
            if 'Subplan Name' in cur_plan:
                if "returns" in cur_plan['Subplan Name']:
                    name = cur_plan['Subplan Name']
                    subplan_name = name[name.index("$"):-1]
                else:
                    subplan_name = cur_plan['Subplan Name']

            current_node = Node(cur_plan['Node Type'], relation_name, schema, alias, group_key, sort_key, join_type,
                                index_name, hash_cond, table_filter, index_cond, merge_cond, recheck_cond, join_filter,
                                subplan_name, actual_rows, actual_time, description)

            # The row number is limited to certain integer
            # if "Limit" == current_node.node_type:
            #     current_node.plan_rows = cur_plan['Plan Rows']

            # if "Scan" in current_node.node_type:
            #     #Index scan
            #     if "Index" in current_node.node_type:
            #         if relation_name:
            #             current_node.set_node_name(
            #                 relation_name + " indexed on " + index_name)
            #     elif "Subquery" in current_node.node_type:
            #         current_node.set_node_name(alias)
            #     else:
            #         current_node.set_node_name(relation_name)
            if 'Plans' in cur_plan:
                child_plans = cur_plan['Plans']
                for child_plan in child_plans:
                    plans.put(child_plan)
                    nodes.put(current_node)
            if parent_node is not None:
                parent_node.add_child(current_node)
            else:
                head = current_node
            
            #Obtained a tree of nodes

        return head

if __name__ == "__main__":
    with open('plan 1.1.json') as json_file:
        data = json.load(json_file)
    head_node = QEP.parse_json_file(data)
    print(head_node.get_node_info())