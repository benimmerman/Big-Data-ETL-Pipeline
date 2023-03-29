import argparse
from pyspark.sql import SparkSession
import ast

def df_create(args_paths_dict):
    keys = list(args_paths_dict)
    df_calendar = spark.read.option("header", "true").option("inferSchema",True).csv(args_paths_dict[keys[0]])
    # df_calendar.show(n=5,truncate=False)
    df_inventory = spark.read.option("header", "true").option("inferSchema",True).csv(args_paths_dict[keys[1]])
    # df_inventory.show(n=5,truncate=False)
    df_product = spark.read.option("header", "true").option("inferSchema",True).csv(args_paths_dict[keys[2]])
    # df_product.show(n=5,truncate=False)
    df_sales = spark.read.option("header", "true").option("inferSchema",True).csv(args_paths_dict[keys[3]])
    # df_sales.show(n=5,truncate=False)
    df_store = spark.read.option("header", "true").option("inferSchema",True).csv(args_paths_dict[keys[4]])
    # df_store.show(n=5,truncate=False)
    return df_calendar, df_inventory, df_product, df_sales, df_store

def transform(dfs):
    dfs[0].createOrReplaceTempView('calendar')
    dfs[1].createOrReplaceTempView('inventory')
    dfs[2].createOrReplaceTempView('product')
    dfs[3].createOrReplaceTempView('sales')
    dfs[4].createOrReplaceTempView('store')

    df_final = spark.sql('''
        with weekly_sales as (
            SELECT yr_wk_num, store_key, prod_key, round(sum(sales_qty),2) as total_sale_qty, 
            round(sum(sales_amt),2) as total_sale_amt, round(sum(sales_amt)/sum(sales_qty),2) as avg_sale_price,
            sum(sales_cost) as cost_of_wk
            FROM sales s
            JOIN calendar c ON s.trans_dt = c.cal_dt
            GROUP BY yr_wk_num, store_key, prod_key
        ),

        weekly_inv as (
            SELECT yr_wk_num, i.store_key, i.prod_key,
            (7-SUM(i.out_of_stock_flg))/7 as prcnt_store_in_stock,
            SUM(i.out_of_stock_flg) as out_of_stock_inst,
            SUM(CASE WHEN i.inventory_on_hand_qty<s.sales_qty THEN 1 ELSE 0 END) as low_stock_flg,
            SUM(CASE WHEN i.inventory_on_hand_qty<s.sales_qty THEN 1 ELSE 0 END)+SUM(i.out_of_stock_flg) as tot_low_stock_impact,
            SUM(CASE WHEN i.out_of_stock_flg=1 THEN sales_amt ELSE 0 END) as no_stock_impact,
            SUM(i.out_of_stock_flg) as no_stock_instances
            FROM inventory i
            JOIN sales s ON i.cal_dt = s.trans_dt
            AND i.store_key = s.store_key
            AND i.prod_key = s.prod_key
            JOIN calendar c ON i.cal_dt = c.cal_dt
            GROUP BY yr_wk_num, i.store_key, i.prod_key
        ),
        eow_data as(
            SELECT c.yr_wk_num, i.store_key, i.prod_key, i.inventory_on_hand_qty as eow_on_hand,
            i.inventory_on_order_qty as eow_on_order
            FROM inventory i
            JOIN calendar c ON i.cal_dt = c.cal_dt
            WHERE i.inventory_on_hand_qty IN (
                SELECT i.inventory_on_hand_qty
                FROM inventory i
                JOIN calendar c ON i.cal_dt = c.cal_dt
                WHERE c.day_of_wk_num=6
                )
            AND i.inventory_on_order_qty IN (
                SELECT i.inventory_on_order_qty
                FROM inventory i
                JOIN calendar c ON i.cal_dt = c.cal_dt
                WHERE c.day_of_wk_num=6
                )
        )

            SELECT weekly_sales.*, prcnt_store_in_stock, out_of_stock_inst, low_stock_flg,
            tot_low_stock_impact, no_stock_impact, no_stock_instances, eow_on_hand, eow_on_order,
            ROUND(eow_on_hand/total_sale_qty,2) as eow_stock_supply,
            CASE WHEN low_stock_flg=1 THEN total_sale_amt-eow_on_hand ELSE 0 END as potential_low_stock_impact
            FROM weekly_sales
            JOIN weekly_inv ON weekly_inv.yr_wk_num=weekly_sales.yr_wk_num
            AND weekly_inv.store_key=weekly_sales.store_key
            AND weekly_inv.prod_key=weekly_sales.prod_key
            JOIN eow_data ON eow_data.yr_wk_num=weekly_sales.yr_wk_num
            AND eow_data.store_key=weekly_sales.store_key
            AND eow_data.prod_key=weekly_sales.prod_key;
    ''')
    return df_final


if __name__ == "__main__":
    parser = argparse.ArgumentParser() 
    parser.add_argument("-p", required=True, help="Spark Input Paramters")
    args = parser.parse_args()
    args_dict = ast.literal_eval(args.p)
    # this takes the string args.p and converts it to a dictionary

    args_paths_list = []
    for key in args_dict.keys():
        args_paths_list.append(args_dict[key])
    args_paths_dict = ast.literal_eval(args_paths_list[0])
    output_path = args_paths_list[1]

    spark = SparkSession.builder.appName('wcd_midterm_job').getOrCreate()

    dfs = df_create(args_paths_dict)
    df_final = transform(dfs)
    df_final.write.option('header','true').parquet(f'{output_path}/business_data.parquet', mode='overwrite')
    dfs[0].write.option('header','true').csv(f'{output_path}calendar.csv', mode='overwrite')
    dfs[4].write.option('header','true').csv(f'{output_path}store.csv', mode='overwrite')
    dfs[2].write.option('header','true').csv(f'{output_path}product.csv', mode='overwrite')