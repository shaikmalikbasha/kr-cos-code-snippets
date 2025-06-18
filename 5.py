def build_select_groupby_from_filters(
    table_alias, filters, column_map, result_alias_map, agg_column_map
):
    keys_to_check = ["div", "store", "modality", "item_consumer_upc", "pos_date"]
    selected_keys = [k for k in keys_to_check if filters.get(k) is not None]
    if not selected_keys:
        selected_keys = keys_to_check

    dim_selects = [
        f"{table_alias}.{column_map[k]} AS {result_alias_map[k]}" for k in selected_keys
    ]
    agg_selects = [
        (
            f"SUM({agg_expr}) AS {agg_alias}"
            if "CASE" in agg_expr
            else f"SUM({table_alias}.{agg_expr}) AS {agg_alias}"
        )
        for agg_alias, agg_expr in agg_column_map.items()
    ]
    select_clause = ",\n    ".join(dim_selects + agg_selects)
    group_by_clause = ", ".join(
        [f"{table_alias}.{column_map[k]}" for k in selected_keys]
    )
    join_keys = selected_keys

    return select_clause, group_by_clause, join_keys


def build_full_mismatch_comparison_query(
    filters,
    dsa_col_map,
    dsr_col_map,
    result_alias_map,
    agg_col_map_dsa,
    agg_col_map_dsr,
):
    dsa_select, dsa_group_by, join_keys = build_select_groupby_from_filters(
        table_alias="dsa",
        filters=filters,
        column_map=dsa_col_map,
        result_alias_map=result_alias_map,
        agg_column_map=agg_col_map_dsa,
    )

    dsr_select, dsr_group_by, _ = build_select_groupby_from_filters(
        table_alias="dsr",
        filters=filters,
        column_map=dsr_col_map,
        result_alias_map=result_alias_map,
        agg_column_map=agg_col_map_dsr,
    )

    # Join ON clause
    join_conditions = " AND ".join(
        [f"dsa.{result_alias_map[k]} = dsr.{result_alias_map[k]}" for k in join_keys]
    )

    # Mismatch conditions
    mismatch_exprs = []
    for col in agg_col_map_dsa.keys():
        mismatch_exprs.append(
            f"""
    SUM(CASE WHEN dsa.{col} != dsr.{col} THEN 1 ELSE 0 END) AS {col}_mismatch,
    COUNT(*) - SUM(CASE WHEN dsa.{col} != dsr.{col} THEN 1 ELSE 0 END) AS {col}_match
"""
        )

    mismatch_section = "".join(mismatch_exprs).strip().rstrip(",")

    full_query = f"""
        WITH DSA_DATASET AS (
            SELECT
            {dsa_select}
            FROM kr_retail_sales_nonprod.release_daily_sales_medallions_blue.daily_sales_v3 dsa
            WHERE dsa.upc_type = 'I'
            AND dsa.pos_date = '{filters.get('pos_date')}'
            GROUP BY {dsa_group_by}
        ),
        DSR_DATASET AS (
            SELECT
            {dsr_select}
            FROM kr_retail_sales_nonprod.trans_aggr_d.item_aggr dsr
            LEFT JOIN kr_retail_sales_nonprod.trans_d.location_division_facility_number_mapping_vw m
                ON dsr.location_id = m.location_id
            WHERE dsr.transaction_date = '{filters.get('pos_date')}'
            GROUP BY {dsr_group_by}
        )
        SELECT
            {mismatch_section}
        FROM
            DSA_DATASET dsa
        JOIN 
            DSR_DATASET dsr
                ON {join_conditions}
        """.strip()

    return full_query


filters = {
    "div": "001",
    "store": None,
    "modality": None,
    "item_consumer_upc": "123456789012",
    "pos_date": "2025-06-16",
}

dsa_col_map = {
    "div": "div",
    "store": "store",
    "modality": "modality",
    "item_consumer_upc": "item_consumer_upc",
    "pos_date": "pos_date",
}

dsr_col_map = {
    "div": "m.division_number",
    "store": "m.facility_number",
    "modality": "dsr.modality",
    "item_consumer_upc": "dsr.upc",
    "pos_date": "dsr.transaction_date",
}

result_alias_map = {
    "div": "div",
    "store": "store",
    "modality": "modality",
    "item_consumer_upc": "item_consumer_upc",
    "pos_date": "pos_date",
}

agg_col_map_dsa = {
    "sum_pos_net_dol_amount": "pos_net_dol_amount",
    "sum_pos_uom_qty": "pos_uom_qty",
    "sum_pos_units": "pos_units",
    "sum_pos_gross_dol_amount": "pos_gross_dol_amount",
    "sum_store_coupon_amount": "store_coupon_amount",
    "sum_manufacturer_coupon_amount": "manufacturer_coupon_amount",
}

agg_col_map_dsr = {
    "sum_pos_net_dol_amount": "net_total_amount",
    "sum_pos_uom_qty": "CASE WHEN dsr.quantity_uom = 'EA' THEN 0 ELSE dsr.quantity_uom_amount END",
    "sum_pos_units": "quantity_uom_amount",
    "sum_pos_gross_dol_amount": "gross_extended_total_amount",
    "sum_store_coupon_amount": "store_coupon_amount",
    "sum_manufacturer_coupon_amount": "manufacturer_coupon_amount",
}

query = build_full_mismatch_comparison_query(
    filters,
    dsa_col_map,
    dsr_col_map,
    result_alias_map,
    agg_col_map_dsa,
    agg_col_map_dsr,
)

print(query)
