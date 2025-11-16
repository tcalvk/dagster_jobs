from typing import List
import dagster as dg

@dg.asset
def hello_world():
    print("Hello, Dagster!")