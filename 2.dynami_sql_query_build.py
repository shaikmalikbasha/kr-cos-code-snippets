def build_dynamic_sql_query(
    table_name, filters, group_by_keys, column_map, result_alias_map
):
    """
    Dynamically builds a SQL query with standardized SELECT and GROUP BY columns.

    Parameters:
    - table_name (str): Table name.
    - filters (dict): Dict with standard keys and their values (e.g., {'div': '001', ...}).
    - group_by_keys (list): List of standard keys to include in SELECT and GROUP BY.
    - column_map (dict): Maps standard keys to actual DB column names.
    - result_alias_map (dict): Maps standard keys to final output names.

    Returns:
    - sql (str): Full SQL query.
    - params (list): Parameters for placeholders.
    - select_clause (str): SELECT columns.
    - group_by_clause (str): GROUP BY columns.
    """

    # SELECT columns with aliasing
    select_parts = [
        f"{column_map[key]} AS {result_alias_map[key]}" for key in group_by_keys
    ] + [
        f"SUM({column_map['total_pos_amt']}) AS total_pos_amt",
        f"SUM({column_map['gross_amt']}) AS gross_amt",
    ]

    # GROUP BY clause using actual table column names
    group_by_clause = ", ".join([column_map[key] for key in group_by_keys])

    # WHERE clause logic
    where_clauses = ["type = 'I'"]
    params = []
    for key, value in filters.items():
        if value is not None and key in column_map:
            where_clauses.append(f"{column_map[key]} = ?")
            params.append(value)

    # Build SQL
    select_clause = ", ".join(select_parts)
    sql = f"""
        SELECT {select_clause}
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        GROUP BY {group_by_clause}
    """

    return sql.strip(), params, select_clause, group_by_clause


filters = {
    "div": "001",
    "store": "123",
    "item_upc_number": "789456",
    "modality": None,
    "pos_date": "2025-06-17",
}

group_by_keys = ["div", "store", "item_upc_number", "pos_date"]

column_map_dsa = {
    "div": "division_id",
    "store": "store_num",
    "item_upc_number": "upc_code",
    "modality": "channel",
    "pos_date": "sales_date",
    "total_pos_amt": "pos_amount",
    "gross_amt": "gross_revenue",
}

result_alias_map = {
    "div": "div",
    "store": "store",
    "item_upc_number": "item_upc_number",
    "modality": "modality",
    "pos_date": "pos_date",
}

sql, params, select_cols, group_by_cols = build_dynamic_sql_query(
    "sales_data", filters, group_by_keys, column_map_dsa, result_alias_map
)
print("SELECT Clause:", select_cols)
print("GROUP BY Clause:", group_by_cols)
