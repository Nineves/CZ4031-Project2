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

    
    
    # for key in plan.keys():
    #     print(key, ":", plan[key])
    # plans = plan['Plans']
    # print(len(plan['Plans']))
    connection.close()
