import json
import argparse
from clickhouse_driver import Client

DATABASE = "fakedb"
# "clickhouse://[user:password]@localhost:9000/default"
CLICKHOUSERURL = "clickhouse://127.0.0.1:9000/default"

def load_parse_to_sql(json_file):
    with open(json_file, "r") as file:
        schemas = json.load(file)

    sqls = [f"create database if not exists {DATABASE};"]
    for table, info in schemas.items():
        sqls += to_sql(table, info["schema"], info["order by"])

    return sqls


def to_sql(table, schema, order_by, limit=1000):
    create_sql = f"""CREATE TABLE IF NOT EXISTS {DATABASE}.{table} 
({", ".join([f"{k} {v}" for k, v in schema.items()])}) 
engine=MergeTree
order by ({", ".join(order_by)});
"""
    insert_sql = f"""INSERT INTO {DATABASE}.{table} 
SELECT * FROM generateRandom() LIMIT {limit};"""

    return [create_sql, insert_sql]


def send_sqls_to_chdb(sqls):
    client = Client.from_url(CLICKHOUSERURL)
    for sql in sqls:
        print(sql)
        client.execute(sql)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gen fake data into ClickHouse for quick tests"
    )
    parser.add_argument("json_file", type=str, help="Path to the JSON file")

    args = parser.parse_args()
    sqls = load_parse_to_sql(args.json_file)
    send_sqls_to_chdb(sqls)
