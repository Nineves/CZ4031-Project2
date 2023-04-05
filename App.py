import database_connection as DC
import json

if __name__ == "__main__":
    connection = DC.DBConnection()
    #result = connection.execute('explain (analyze, costs, verbose, buffers, format json) select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10')[0][0][0]
    result = connection.execute("explain (analyze, costs, verbose, buffers, format json) select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10 order by customer.c_custkey")[0][0][0]
    with open('plan 1.1.json', 'w',newline='\r\n') as f:
        json.dump(result, f,indent=2)

    result2 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_custkey >= 75000 and nation.n_nationkey >= 10 order by nation.n_name")[0][0][0]
    with open('plan 1.2.json', 'w',newline='\r\n') as f:
        json.dump(result2, f,indent=2)
    
    result3 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select o_orderpriority,count(*) as order_count from orders where o_totalprice > 100 and exists (select * from lineitem where l_orderkey = o_orderkey and l_extendedprice > 100) group by o_orderpriority order by o_orderpriority;")[0][0][0]
    with open('plan 2.json', 'w',newline='\r\n') as f:
        json.dump(result3, f,indent=2)

    result4 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select orders.o_orderkey, customer.c_nationkey, supplier.s_nationkey from orders, customer, supplier, lineitem where customer.c_custkey = orders.o_custkey and orders.o_orderkey = lineitem.l_orderkey and lineitem.l_suppkey = supplier.s_suppkey and supplier.s_nationkey = customer.c_nationkey")[0][0][0]
    with open('plan 3.1.json', 'w',newline='\r\n') as f:
        json.dump(result4, f,indent=2)

    result44 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select orders.o_orderkey, customer.c_nationkey, supplier.s_nationkey from orders, customer, supplier, lineitem where customer.c_custkey = orders.o_custkey and orders.o_orderkey = lineitem.l_orderkey and lineitem.l_suppkey = supplier.s_suppkey and supplier.s_nationkey = customer.c_nationkey and orders.o_orderkey < 100")[0][0][0]
    with open('plan 3.3.json', 'w',newline='\r\n') as f:
        json.dump(result44, f,indent=2)

    result5 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select orders.o_orderkey, customer.c_nationkey, supplier.s_nationkey from orders, customer, supplier, lineitem where customer.c_custkey = orders.o_custkey and orders.o_orderkey = lineitem.l_orderkey and lineitem.l_suppkey = supplier.s_suppkey and supplier.s_nationkey != customer.c_nationkey")[0][0][0]
    with open('plan 3.2.json', 'w',newline='\r\n') as f:
        json.dump(result5, f,indent=2)

    result6 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select supplier.s_name, supplier.s_acctbal from nation, supplier, lineitem where nation.n_name = 'JAPAN' and supplier.s_nationkey = nation.n_nationkey and lineitem.l_suppkey = supplier.s_suppkey and lineitem.l_quantity = 30")[0][0][0]
    with open('plan 4.1.json', 'w',newline='\r\n') as f:
        json.dump(result6, f,indent=2)

    result7 = connection.execute("explain (analyze, costs, verbose, buffers, format json) select supplier.s_name, supplier.s_acctbal from nation, supplier, lineitem where nation.n_name != 'JAPAN' and supplier.s_nationkey = nation.n_nationkey and lineitem.l_suppkey = supplier.s_suppkey and lineitem.l_quantity = 30")[0][0][0]
    with open('plan 4.2.json', 'w',newline='\r\n') as f:
        json.dump(result7, f,indent=2)
    
    # for key in plan.keys():
    #     print(key, ":", plan[key])
    # plans = plan['Plans']
    # print(len(plan['Plans']))
    connection.close()
