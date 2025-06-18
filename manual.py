filter = {
    "div": "001",
    "store": None,
    "modality": None,
    "item_consumer_upc": "123456789012",
    "pos_date": "2025-06-16",
}

dsa_col_map = {
    "div": "dsa.div",
    "store": "dsa.store",
    "modality": "dsa.modality",
    "item_consumer_upc": "dsa.item_consumer_upc",
    "pos_date": "dsa.pos_date",
}

dsr_col_map = {
    "div": "m.division_number",
    "store": "m.facility_number",
    "modality": "dsr.modality",
    "item_consumer_upc": "dsr.upc",
    "pos_date": "dsr.transaction_date",
}

agg_col_map_dsa = {
    "sum_pos_net_dol_amount": "dsa.pos_net_dol_amount",
    "sum_pos_uom_qty": "dsa.pos_uom_qty",
    "sum_pos_units": "dsa.pos_units",
    "sum_pos_gross_dol_amount": "dsa.pos_gross_dol_amount",
    "sum_store_coupon_amount": "dsa.store_coupon_amount",
    "sum_manufacturer_coupon_amount": "dsa.manufacturer_coupon_amount",
}

agg_col_map_dsr = {
    "sum_pos_net_dol_amount": "dsr.net_total_amount",
    "sum_pos_uom_qty": "CASE WHEN dsr.quantity_uom = 'EA' THEN 0 ELSE dsr.quantity_uom_amount END",
    "sum_pos_units": "dsr.quantity_uom_amount",
    "sum_pos_gross_dol_amount": "dsr.gross_extended_total_amount",
    "sum_store_coupon_amount": "dsr.store_coupon_amount",
    "sum_manufacturer_coupon_amount": "dsr.manufacturer_coupon_amount",
}


def get_select_clause_and_group_by(
    filters, column_map, result_alias_map, agg_column_map
):
    """
    Build the SELECT clause and GROUP BY clause for a SQL query.

    Args:
    - filters (dict): Dictionary of filter values.
    - column_map (dict): Mapping of filter keys to actual column names.
    - result_alias_map (dict): Mapping of filter keys to result aliases.
    - agg_column_map (dict): Mapping of aggregate column names to expressions.
    - table_alias (str): Alias for the table in the SQL query.

    Returns:
    - select_clause (str)
    - group_by_clause (str)
    - selected_keys (list): the keys used in group by
    """

    # Use only non-null filters for GROUP BY
    selected_keys = [key for key in filters if filters[key] is not None]

    # SELECT dimensions
    dim_selects = [
        f"{column_map[key]} AS {result_alias_map[key]}" for key in selected_keys
    ]

    # Aggregates
    agg_selects = [
        (
            f"SUM({agg_expr}) AS {agg_alias}"
            if "CASE" not in agg_expr
            else f"SUM({agg_expr}) AS {agg_alias}"
        )
        for agg_alias, agg_expr in agg_column_map.items()
    ]

    # GROUP BY
    group_by_clause = ", ".join([f"{column_map[key]}" for key in selected_keys])
    select_clause = ",\n    ".join(dim_selects + agg_selects)

    return select_clause, group_by_clause, selected_keys


select_clause, group_by_clause, selected_keys = get_select_clause_and_group_by(
    filters=filter,
    column_map=dsa_col_map,
    result_alias_map=dsa_col_map,
    agg_column_map=agg_col_map_dsa,
)
print("SELECT Clause:")
print(select_clause)
print("\nGROUP BY Clause:")
print(group_by_clause)
print("\nSelected Keys:")
print(selected_keys)
print("\n---\n")

select_clause, group_by_clause, selected_keys = get_select_clause_and_group_by(
    filters=filter,
    column_map=dsr_col_map,
    result_alias_map=dsr_col_map,
    agg_column_map=agg_col_map_dsr,
)
print("SELECT Clause:")
print(select_clause)
print("\nGROUP BY Clause:")
print(group_by_clause)
print("\nSelected Keys:")
print(selected_keys)
print("\n---\n")
