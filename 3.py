def build_select_groupby_for_dataset(
    table_alias: str,
    group_by_keys: list,
    column_map: dict,
    result_alias_map: dict,
    agg_column_map: dict,
):
    """
    Builds SELECT and GROUP BY clause for DSA or DSR dataset.

    Parameters:
    - table_alias (str): alias like 'dsa' or 'dsr'
    - group_by_keys (list): standard keys like ['div', 'store', ...]
    - column_map (dict): maps standard keys to actual column names
    - result_alias_map (dict): maps standard keys to final aliases (for select)
    - agg_column_map (dict): maps aggregation keys (e.g. sum_x) to actual column names

    Returns:
    - select_clause (str)
    - group_by_clause (str)
    """

    # Dimension selects with aliases
    dim_selects = [
        f"{table_alias}.{column_map[key]} AS {result_alias_map[key]}"
        for key in group_by_keys
    ]

    # Aggregation expressions
    agg_selects = [
        f"SUM({table_alias}.{agg_col}) AS {agg_alias}"
        for agg_alias, agg_col in agg_column_map.items()
    ]

    # Group By columns
    group_by_clause = ", ".join(
        [f"{table_alias}.{column_map[key]}" for key in group_by_keys]
    )
    select_clause = ",\n    ".join(dim_selects + agg_selects)

    return select_clause, group_by_clause


group_by_keys = ["pos_date", "item_consumer_upc", "div", "store", "modality"]

column_map_dsa = {
    "pos_date": "pos_date",
    "item_consumer_upc": "item_consumer_upc",
    "div": "div",
    "store": "store",
    "modality": "modality",
}

column_map_dsr = {
    "pos_date": "transaction_date",
    "item_consumer_upc": "upc",
    "div": "division_number",
    "store": "facility_number",
    "modality": "modality",
}

result_alias_map = {
    "pos_date": "pos_date",
    "item_consumer_upc": "item_consumer_upc",
    "div": "div",
    "store": "store",
    "modality": "modality",
}

agg_column_map_dsa = {
    "sum_pos_net_dol_amount": "pos_net_dol_amount",
    "sum_pos_uom_qty": "pos_uom_qty",
    "sum_pos_units": "pos_units",
    "sum_pos_gross_dol_amount": "pos_gross_dol_amount",
    "sum_store_coupon_amount": "store_coupon_amount",
    "sum_manufacturer_coupon_amount": "manufacturer_coupon_amount",
}

agg_column_map_dsr = {
    "sum_pos_net_dol_amount": "net_total_amount",
    "sum_pos_uom_qty": "CASE WHEN dsr.quantity_uom = 'EA' THEN 0 ELSE dsr.quantity_uom_amount END",
    "sum_pos_units": "quantity_uom_amount",
    "sum_pos_gross_dol_amount": "gross_extended_total_amount",
    "sum_store_coupon_amount": "store_coupon_amount",
    "sum_manufacturer_coupon_amount": "manufacturer_coupon_amount",
}
select_dsa, groupby_dsa = build_select_groupby_for_dataset(
    table_alias="dsa",
    group_by_keys=group_by_keys,
    column_map=column_map_dsa,
    result_alias_map=result_alias_map,
    agg_column_map=agg_column_map_dsa,
)

select_dsr, groupby_dsr = build_select_groupby_for_dataset(
    table_alias="dsr",
    group_by_keys=group_by_keys,
    column_map=column_map_dsr,
    result_alias_map=result_alias_map,
    agg_column_map=agg_column_map_dsr,
)

print("-- DSA SELECT:\n", select_dsa)
print("-- DSA GROUP BY:\n", groupby_dsa)
print("-- DSR SELECT:\n", select_dsr)
print("-- DSR GROUP BY:\n", groupby_dsr)
