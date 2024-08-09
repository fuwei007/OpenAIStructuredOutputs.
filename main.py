
# create the test DB with the same schama
# --------------------------------------------
import sqlite3
from datetime import datetime, timedelta
import random

def setup_database():
    # Connect to SQLite database in memory
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Create table 'orders'
    cursor.execute('''
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        ordered_at TEXT,
        shipped_at TEXT,
        delivered_at TEXT,
        expected_delivery_date TEXT,
        status TEXT
    )
    ''')

    # Generate sample data
    status_options = ['fulfilled', 'processing', 'cancelled', 'shipped']
    base_date = datetime.strptime('2023-05-01', '%Y-%m-%d')
    orders = []

    for i in range(1, 101):  # Generating 100 orders
        ordered_date = base_date + timedelta(days=random.randint(0, 30))
        shipped_date = ordered_date + timedelta(days=random.randint(1, 3))
        expected_delivery_date = shipped_date + timedelta(days=random.randint(2, 10))
        delivered_date = expected_delivery_date + timedelta(days=random.randint(-2, 5))
        status = random.choice(status_options)

        # Randomly decide if the order is still pending or was delivered/cancelled
        if status == 'processing' or status == 'cancelled':
            delivered_date = None  # No delivery date if processing or cancelled

        orders.append((
            i,
            ordered_date.strftime('%Y-%m-%d'),
            shipped_date.strftime('%Y-%m-%d'),
            None if delivered_date is None else delivered_date.strftime('%Y-%m-%d'),
            expected_delivery_date.strftime('%Y-%m-%d'),
            status
        ))

    # Insert data into the 'orders' table
    cursor.executemany('INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)', orders)

    # Save (commit) the changes
    conn.commit()

    return conn  # Return the connection to use later


def run_query(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    for result in results:
        print(result)


# Example usage
conn = setup_database()

# --------------------------------------------



from enum import Enum
from typing import Union

from pydantic import BaseModel

import openai
from openai import OpenAI

import os
os.environ['OPENAI_API_KEY'] = ''


class Table(str, Enum):
    orders = "orders"
    customers = "customers"
    products = "products"

class Column(str, Enum):
    id = "id"
    status = "status"
    expected_delivery_date = "expected_delivery_date"
    delivered_at = "delivered_at"
    shipped_at = "shipped_at"
    ordered_at = "ordered_at"
    canceled_at = "canceled_at"

class Operator(str, Enum):
    eq = "="
    gt = ">"
    lt = "<"
    le = "<="
    ge = ">="
    ne = "!="
class OrderBy(str, Enum):
    asc = "asc"
    desc = "desc"

class DynamicValue(BaseModel):
    column_name: str

class Condition(BaseModel):
    column: str
    operator: Operator
    value: Union[str, int, DynamicValue]

class Query(BaseModel):
    table_name: Table
    columns: list[Column]
    conditions: list[Condition]
    order_by: OrderBy

schemas = openai.pydantic_function_tool(Query)
print(schemas)

client = OpenAI()

completion = client.beta.chat.completions.parse(
    model="gpt-4o-2024-08-06",
    messages=[
        {
            "role": "system",
            "content": "You are a helpful assistant. The current date is August 6, 2024. You help users query for the data"
                       " they are looking for by calling the query function.",
        },
        {
            "role": "user",
            "content": "look up all my orders in May of last year that were fulfilled but not delivered on time",
        },
    ],
    tools=[
        schemas,
    ],
)

parsed_arguments = completion.choices[0].message.tool_calls[0].function.parsed_arguments
print(parsed_arguments)

from typing import List
def generate_sql_query(table_name: Table, columns: List[Column], conditions: List[Condition], order_by: OrderBy) -> str:
    # Start constructing the SQL SELECT statement
    column_names = ', '.join([column.value for column in columns])  # Extract column names from the enum values
    sql = f"SELECT {column_names} FROM {table_name.value}"

    # Add WHERE clause if conditions are provided
    if conditions:
        condition_statements = []
        for condition in conditions:
            if isinstance(condition.value, DynamicValue):
                condition_value = condition.value.column_name  # Handle dynamic value for the column
            else:
                condition_value = f"'{condition.value}'" if isinstance(condition.value, str) else condition.value
            condition_statements.append(f"{condition.column} {condition.operator.value} {condition_value}")
        conditions_sql = ' AND '.join(condition_statements)
        sql += f" WHERE {conditions_sql}"

    # Add ORDER BY clause
    sql += f" ORDER BY {columns[0].value} {order_by.value}"  # Assuming ordering by the first column provided

    return sql + ';'

queryStr = generate_sql_query(parsed_arguments.table_name, parsed_arguments.columns, parsed_arguments.conditions, parsed_arguments.order_by)
print(queryStr)


run_query(conn, queryStr)


