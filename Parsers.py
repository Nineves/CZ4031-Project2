import queue
import json
from igraph import Graph
import plotly.graph_objects as go


class Node(object):
    def __init__(self, node_number, total_cost, node_type, relation_name, schema, alias, group_key, sort_key, 
                 join_type, index_name, hash_cond, table_filter, index_cond, merge_cond, recheck_cond, join_filter, 
                 subplan_name, actual_rows, actual_time,description):
        self.node_number = node_number #unique index number for each node
        self.total_cost = total_cost
        self.inter_name = None
        self.cur_cost = 0
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
        self.step = None
        
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
    
    def set_inter_name(self, inter_name):
        self.inter_name = inter_name
    
    def set_node_cost(self, cost):
        self.cur_cost = cost
    
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
        print("Node index:{}".format(str(self.node_number)))
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
        self.num_of_nodes = len(self.all_nodes)
    

    
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

         
    def get_node(self, node_index):
        nodes = queue.Queue()
        nodes.put(self.head_node)
        while not nodes.empty():
            cur_node = nodes.get()
            if cur_node.node_number == node_index:
                return cur_node
            for child in cur_node.child_nodes:
                nodes.put(child)

    def plot(self, diff_node_indexes = []):
        '''
        Pass in the indexes of evolved nodes, which will be marked as yellow in the visualization.
        '''
        graph = [[0 for i in range(self.num_of_nodes)] for j in range(self.num_of_nodes)]
        for node in self.all_nodes:
            for child in node.child_nodes:
                graph[node.node_number - 1][child.node_number - 1] = 1
        
        G = Graph.Adjacency(graph)
        layout = G.layout('tree')

        position = {k: layout[k] for k in range(self.num_of_nodes)}
        Ys = [layout[k][1] for k in range(self.num_of_nodes)]
        maxY = max(Ys)
        edges = [e.tuple for e in G.es]
        Xn = [position[k][0] for k in range(self.num_of_nodes)]
        Yn = [2 * maxY - position[k][1] for k in range(self.num_of_nodes)]
        Xe = []
        Ye = []
        for edge in edges:
            Xe += [position[edge[0]][0], position[edge[1]][0], None]
            Ye += [2 * maxY - position[edge[0]][1],
                   2 * maxY - position[edge[1]][1], None]
        
        sorted_nodes = sorted(self.all_nodes, key = lambda x:x.node_number)
        labels_name = []
        hovered_text = []
        index = []
        self.generate_NLP_description()
        for i in range(len(sorted_nodes)):
            index.append(str(sorted_nodes[i].node_number))
            labels_name.append(str(sorted_nodes[i].node_number) + " " + sorted_nodes[i].node_type)
            if sorted_nodes[i].step is not None:
                hovered_text.append(sorted_nodes[i].step)
            else:
                hovered_text.append("")
        
        sorted_diff_nodes = sorted(diff_node_indexes)
        Xdiff = []
        Ydiff = []
        hovered_diff_text = []
        if len(sorted_diff_nodes) != 0:
            for i in range(len(sorted_diff_nodes)):
                Xdiff.append(position[sorted_diff_nodes[i]-1][0])
                Ydiff.append(2 * maxY - position[sorted_diff_nodes[i]-1][1])
                hovered_diff_text.append(self.get_node(sorted_diff_nodes[i]).step)

        fig = go.Figure()
        fig.add_trace(go.Scatter(x=Xe,
                                 y=Ye,
                                 mode='lines',
                                 line=dict(color='rgb(210,210,210)', width=2),
                                 hoverinfo='none'
                                 ))
        fig.add_trace(go.Scatter(x=Xn,
                                 y=Yn,
                                 mode='markers + text',
                                 name='bla',
                                 marker=dict(symbol='diamond-wide',
                                             size=70,
                                             color='#9b59b6',  # '#DB4551',
                                             line=dict(
                                                 color='rgb(50,50,50)', width=2)
                                             ),
                                 #text=labels_name,
                                 hoverinfo='text',
                                 hovertext=hovered_text,
                                 opacity=1,
                                 textposition="bottom center"
                                 ))
        fig.add_trace(go.Scatter(x=Xdiff,
                                 y=Ydiff,
                                 mode='markers + text',
                                 name='bla',
                                 marker=dict(symbol='diamond-wide',
                                             size=70,
                                             color='#f1c40f',  # '#DB4551',
                                             line=dict(
                                                 color='rgb(50,50,50)', width=2)
                                             ),
                                 #text=labels_name,
                                 hoverinfo='text',
                                 hovertext=hovered_diff_text,
                                 opacity=0.8,
                                 textposition="bottom center"
                                 ))
        fig.update_traces(textposition='top center')
        axis = dict(showline=False,  # hide axis line, grid, ticklabels and  title
                    zeroline=False,
                    showgrid=False,
                    showticklabels=True,
                    )

        fig.update_layout(title='Tree View of Query Plan',
                          annotations=self.make_annotations(
                              position, self.all_nodes, maxY),
                          font_size=12,
                          showlegend=False,
                          xaxis=axis,
                          yaxis=axis,
                          margin=dict(l=20, r=20, b=20, t=100),
                          hovermode='closest',
                          plot_bgcolor='rgb(248,248,248)'
                          )
        fig.show()

    
    def make_annotations(self, position, nodes, M, font_size=10, font_color='rgb(250,250,250)'):
        ''' Include the annotations of that particular step of qep in node '''
        L = len(position)
        sorted_nodes = sorted(nodes, key = lambda x:x.node_number)
        if len(nodes) != L:
            raise ValueError('The lists pos and text must have the same len')
        annotations = []
        for k in range(L):
            annotations.append(
                dict(
                    # or replace labels with a different list for the text within the circle
                    text=str(k+1) + " " + sorted_nodes[k].node_type,
                    x=position[k][0], y=2 * M - position[k][1],
                    xref='x1', yref='y1',
                    font=dict(color=font_color, size=font_size),
                    showarrow=False)
            )
        return annotations
    
    def generate_NLP_description(self):
        steps = {}
        visited = []
        stack = []
        stack.insert(0, self.head_node)
        intermediate_count = 1
        counter = 1
        while not len(stack) == 0:
            cur_node = stack[0]
            if len(cur_node.child_nodes) == 0 or cur_node.node_number in visited: #leaf node or all of its child nodes have been examined
                NLP_description = ""
                if "Bitmap Index Scan" in cur_node.node_type or "Gather" in cur_node.node_type : #skip
                    stack.pop(0)
                    continue
                elif "Bitmap Heap Scan" in cur_node.node_type:
                    if len(cur_node.child_node) > 0 and "Bitmap Index Scan" in cur_node.child_node[0].node_type:
                        NLP_description += "Perform bitmap heap scan on table {} with index on condition. The scan result is named as {}.".format(cur_node.child_node[0].relation_name, cur_node.child_node[0].node.recheck_cond, "T"+str(intermediate_count))
                        cur_node.child_node[0].set_inter_name("T"+str(intermediate_count))
                    else:
                        NLP_description += "Perform bitmap heap scan on table {}. The scan result is named as {}.".format(cur_node.relation_name, "T"+str(intermediate_count))
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                    
                elif "Scan" in cur_node.node_type:
                    NLP_description += "Perform {} on table {}. The scan result is named as {}".format(cur_node.node_type, cur_node.relation_name, "T"+str(intermediate_count))
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                    
                elif "Join" in cur_node.node_type:
                    NLP_description += "Perform {} on ".format(cur_node.node_type)
                    for child_node in cur_node.child_nodes:
                        if child_node.inter_name != None:
                            NLP_description += "{}, ".format(child_node.inter_name)
                        else:
                            NLP_description += "result after {}, ".format(child_node.node_type())
                    NLP_description += ". The join result is named as {}".format("T"+str(intermediate_count))
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                    

                elif "Nested Loop" in cur_node.node_type:
                    NLP_description += "Perform Nested Loop Join on "
                    for child_node in cur_node.child_nodes:
                        if child_node.inter_name != None:
                            NLP_description += child_node.inter_name
                        else:
                            NLP_description += "result after {}, ".format(child_node.node_type())
                    NLP_description += ". The join result is named as {}".format("T"+str(intermediate_count))
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                    
                    
                elif "Sort" in cur_node.node_type:
                    NLP_description += "Perform Sort on {} with sort key {}. ".format(cur_node.child_nodes[0].inter_name, cur_node.sort_key)
                    NLP_description += "The sorted result is named as {}. ".format("T"+str(intermediate_count))
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                    

                elif "Aggregate" in cur_node.node_type:
                    NLP_description += "Perform Aggregate on {}".format(cur_node.child_nodes[0].inter_name)
                    if len(cur_node.child_nodes) == 2:
                        NLP_description += " and {}.".format(cur_node.child_nodes[1].inter_name) 
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                    
                
                elif "Unique" in cur_node.node_type:
                    NLP_description += "Select unique tuple on {}".format(cur_node.child_nodes[0].inter_name)
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1
                   
                
                else:
                    NLP_description += "Perform {} on ".format(cur_node.node_type)
                    for child_node in cur_node.child_nodes:
                        if child_node.inter_name != None:
                            NLP_description += child_node.inter_name
                        else:
                            NLP_description += "result after {}, ".format(child_node.node_type())
                    NLP_description += ". The {} result is named as {}".format(cur_node.node_type, "T"+str(intermediate_count))
                    cur_node.set_inter_name("T"+str(intermediate_count))
                    intermediate_count += 1

                if cur_node.group_key:
                    print(cur_node.group_key)
                    NLP_description += " with grouping on attribute " + cur_node.group_key[0]

                if cur_node.table_filter:
                    NLP_description += ", filtering on " + cur_node.table_filter

                if cur_node.join_filter:
                    NLP_description += ", join filtering on " +  cur_node.join_filter
                steps[str(counter)] = NLP_description
                cur_node.step = NLP_description
                counter += 1
                stack.pop(0)
            else:
                visited.append(cur_node.node_number)
                stack = cur_node.child_nodes + stack
        return steps
        

    @staticmethod
    def parse_json_file(json_file):
        """
        Used for parsing query plan to query plan tree
        """
        plan_to_parse = json_file['Plan']
        plans = queue.Queue()
        nodes = queue.Queue()
        parent_node = None
        node_index = 0

        plans.put(plan_to_parse)

        while not plans.empty():
            cur_plan = plans.get()
            if not nodes.empty():
                parent_node = nodes.get()
            node_index += 1

            #Initialize current node
            relation_name = schema = alias = group_key = sort_key = join_type = index_name = hash_cond = table_filter \
            = index_cond = merge_cond = recheck_cond = join_filter = subplan_name = actual_rows = actual_time = description = total_cost = None
            if 'Relation Name' in cur_plan:
                relation_name = cur_plan['Relation Name']
            if 'Total Cost' in cur_plan:
                total_cost = cur_plan['Total Cost']
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

            current_node = Node(node_index, total_cost, cur_plan['Node Type'], relation_name, schema, alias, group_key, sort_key, join_type,
                                index_name, hash_cond, table_filter, index_cond, merge_cond, recheck_cond, join_filter,
                                subplan_name, actual_rows, actual_time, description)

            if "Limit" == current_node.node_type:
                 current_node.plan_rows = cur_plan['Plan Rows']

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
    with open('plan 3.1.json') as json_file:
        data = json.load(json_file)
    head_node = QEP.parse_json_file(data)
    new_QEP = QEP(head_node)
    new_QEP.plot([1,3])
