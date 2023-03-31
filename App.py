import database_connection as DC
import json

if __name__ == "__main__":
    connection = DC.DBConnection()
    #result = connection.execute('explain (analyze, costs, verbose, buffers, format json) select customer.c_custkey, customer.c_name, nation.n_name from customer, nation where customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10')[0][0][0]
    result = connection.execute("explain (analyze, costs, verbose, buffers, format json) SELECT COUNT(DT.c_name) AS name_count FROM (SELECT customer.c_custkey, customer.c_name, nation.n_name from customer, nation WHERE customer.c_nationkey = nation.n_nationkey and customer.c_acctbal >= 1000 and nation.n_nationkey >= 10) AS DT")[0][0][0]
    plan = result['Plan']
    with open('plan 1.json', 'w') as f:
        json.dump(plan, f)
    
    # for key in plan.keys():
    #     print(key, ":", plan[key])
    # plans = plan['Plans']
    # print(len(plan['Plans']))
    connection.close()
