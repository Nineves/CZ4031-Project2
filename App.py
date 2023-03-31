import database_connection as DC
import json

if __name__ == "__main__":
    connection = DC.DBConnection()
    #result = connection.execute('explain (analyze, costs, verbose, buffers, format json) select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10')[0][0][0]
    result = connection.execute("explain (analyze, costs, verbose, buffers, format json) SELECT COUNT(DT.c_name) AS name_count FROM (SELECT customer.c_custkey, customer.c_name, nation.n_name from customer, nation WHERE customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10) AS DT")[0][0][0]
    with open('plan 1.1.json', 'w',newline='\r\n') as f:
        json.dump(result, f,indent=2)

    result2 = connection.execute("explain (analyze, costs, verbose, buffers, format json) SELECT COUNT(DT.c_name) AS name_count FROM (SELECT customer.c_custkey, customer.c_name, nation.n_name, orders.o_orderkey from customer, nation, orders WHERE customer.c_nationkey = nation.n_nationkey and customer.c_custkey = orders.o_custkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10) AS DT")[0][0][0]
    with open('plan 1.2.json', 'w',newline='\r\n') as f:
        json.dump(result2, f,indent=2)
    
    # for key in plan.keys():
    #     print(key, ":", plan[key])
    # plans = plan['Plans']
    # print(len(plan['Plans']))
    connection.close()
