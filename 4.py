def build_select_groupby_from_filters(
    table_alias: str,
    filters: dict,
    column_map: dict,
    result_alias_map: dict,
    agg_column_map: dict,
):
    """
    Builds dynamic SELECT and GROUP BY clauses based on which filters were provided.

    Parameters:
    - table_alias (str): e.g. 'dsa' or 'dsr'
    - filters (dict): e.g. {'div': '001', 'store': None, 'modality': 'A'}
    - column_map (dict): maps logical column names to physical ones
    - result_alias_map (dict): maps logical names to result column aliases
    - agg_column_map (dict): {agg_alias: physical_column or SQL expression}

    Returns:
    - select_clause (str)
    - group_by_clause (str)
    - selected_keys (list): the keys used in group by
    """

    # Use only non-null filters for GROUP BY
    selected_keys = [key for key in filters if filters[key] is not None]

    # SELECT dimensions
    dim_selects = [
        f"{table_alias}.{column_map[key]} AS {result_alias_map[key]}"
        for key in selected_keys
    ]

    # Aggregates
    agg_selects = [
        (
            f"SUM({table_alias}.{agg_expr}) AS {agg_alias}"
            if "CASE" not in agg_expr
            else f"SUM({agg_expr}) AS {agg_alias}"
        )
        for agg_alias, agg_expr in agg_column_map.items()
    ]

    # GROUP BY
    group_by_clause = ", ".join(
        [f"{table_alias}.{column_map[key]}" for key in selected_keys]
    )
    select_clause = ",\n    ".join(dim_selects + agg_selects)

    return select_clause, group_by_clause, selected_keys


filters = {
    "div": "001",
    "store": None,
    "modality": "A",
    "item_consumer_upc": "123456789012",
    "pos_date": "2025-06-16",
}

column_map_dsa = {
    "div": "div",
    "store": "store",
    "modality": "modality",
    "item_consumer_upc": "item_consumer_upc",
    "pos_date": "pos_date",
}

result_alias_map = {
    "div": "div",
    "store": "store",
    "modality": "modality",
    "item_consumer_upc": "item_consumer_upc",
    "pos_date": "pos_date",
}

agg_column_map_dsa = {
    "sum_pos_net_dol_amount": "pos_net_dol_amount",
    "sum_pos_uom_qty": "pos_uom_qty",
    "sum_pos_units": "pos_units",
    "sum_pos_gross_dol_amount": "pos_gross_dol_amount",
    "sum_store_coupon_amount": "store_coupon_amount",
    "sum_manufacturer_coupon_amount": "manufacturer_coupon_amount",
}

select_clause, group_by_clause, used_keys = build_select_groupby_from_filters(
    table_alias="dsa",
    filters=filters,
    column_map=column_map_dsa,
    result_alias_map=result_alias_map,
    agg_column_map=agg_column_map_dsa,
)

print("SELECT Clause:\n", select_clause)
print("GROUP BY Clause:\n", group_by_clause)
print("Used Group By Keys:", used_keys)
