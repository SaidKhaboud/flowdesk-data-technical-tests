"""Module that contains a Pandas to BigQuery schema translator."""

import datetime
import pandas as pd
from typing import List

data = [
    {
        "column1": 10,
        "column2": 3.14,
        "column3": {"nested1": "A", "nested2": {"nested3": True}},
        "column4": ["X", "Y"],
        "column5": datetime.datetime(2023, 1, 1),
    },
    {
        "column1": 20,
        "column2": 2.71,
        "column3": {"nested1": "B", "nested2": {"nested3": False}},
        "column4": ["Z"],
        "column5": datetime.datetime(2023, 2, 1),
    },
    {
        "column1": 30,
        "column2": 1.23,
        "column3": {"nested1": "C", "nested2": {"nested3": True}},
        "column4": ["W", "V"],
        "column5": datetime.datetime(2023, 3, 1),
    },
]
df = pd.DataFrame(data)
#print(df)

kind_To_Type = {
    "i": "INTEGER",
    "u": "NUMERIC",
    "b": "BOOLEAN",
    "f": "FLOAT",
    "O": "STRING",
    "S": "STRING",
    "U": "STRING",
    "M": "TIMESTAMP",
}
def generate_bigquery_schema_from_pandas(df: pd.DataFrame) -> List[dict]:
    schema = []
    for col_name, col_type in zip(df.columns, df.dtypes):
        val = df[col_name].iloc[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"
        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(val[0], dict)):
            fields = generate_bigquery_schema_from_pandas(pd.json_normalize(val))
        else:
            fields = ()
        
        type = "RECORD" if fields else kind_To_Type.get(col_type.kind)
        
        if fields:
                schema.append(
                {"name":col_name,
                "type": type,
                "mode": mode,
                "fields": fields}
            )
        else:
            schema.append(
                {"name":col_name,
                "type": type,
                "mode": mode}
            )
    
    return schema

schema = generate_bigquery_schema_from_pandas(df)
print(schema)
