def build_dynamic_sql_query(table_name, filters, column_map, result_alias_map):
    """
    Builds a dynamic SQL query that standardizes column names and outputs key components.

    Returns:
    - sql (str): Full SQL query.
    - params (list): Parameters for placeholders.
    - select_clause (str): Comma-separated list of SELECT columns with aliases.
    - group_by_clause (str): Comma-separated list of GROUP BY columns.
    """
    group_by_keys = ["div", "store", "item_upc_number", "modality", "pos_date"]

    selected_group_by = [col for col in group_by_keys if filters.get(col) is not None]
    if not selected_group_by:
        selected_group_by = group_by_keys.copy()

    # SELECT clause
    select_parts = [
        f"{column_map[col]} AS {result_alias_map[col]}" for col in selected_group_by
    ]
    select_parts += [
        f"SUM({column_map['total_pos_amt']}) AS total_pos_amt",
        f"SUM({column_map['gross_amt']}) AS gross_amt",
    ]
    select_clause = ", ".join(select_parts)

    # WHERE clause
    where_clauses = ["type = 'I'"]
    params = []
    for col in group_by_keys:
        if filters.get(col) is not None:
            where_clauses.append(f"{column_map[col]} = ?")
            params.append(filters[col])

    # GROUP BY clause
    group_by_clause = ", ".join([column_map[col] for col in selected_group_by])

    sql = f"""
        SELECT {select_clause}
        FROM {table_name}
        WHERE {' AND '.join(where_clauses)}
        GROUP BY {group_by_clause}
    """

    return sql.strip(), params, select_clause, group_by_clause


if __name__ == "__main__":
    filters = {
        "div": "001",
        "store": None,
        "item_upc_number": "123456",
        "modality": None,
        "pos_date": "2025-06-17",
    }

    column_map_dsa = {
        "div": "div",
        "store": "store",
        "item_upc_number": "upc",
        "modality": "modality",
        "pos_date": "pos_date",
        "total_pos_amt": "pos_amount",
        "gross_amt": "gross_sales",
    }
    column_map_dsr = {
        "div": "division_number",
        "store": "facility_number",
        "item_upc_number": "upc",
        "modality": "modality",
        "pos_date": "transaction_date",
        "total_pos_amt": "pos_amount",
        "gross_amt": "gross_sales",
    }

    result_alias_map = {
        "div": "div",
        "store": "store",
        "item_upc_number": "item_upc_number",
        "modality": "modality",
        "pos_date": "pos_date",
    }

    dsa_sql, dsa_params, dsa_select_cols, dsa_group_by_cols = build_dynamic_sql_query(
        "sales_data", filters, column_map_dsa, result_alias_map
    )
    dsr_sql, dsr_params, dsr_select_cols, dsr_group_by_cols = build_dynamic_sql_query(
        "sales_data", filters, column_map_dsr, result_alias_map
    )

    print("SQL:\n", dsa_sql)
    print("Params:", dsa_params)
    print("SELECT clause:", dsa_select_cols)
    print("GROUP BY clause:", dsa_group_by_cols)
