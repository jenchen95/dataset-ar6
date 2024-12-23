import polars as pl 
import numpy as np
from func import fit_spline

ele_gen = (
        pl.read_parquet('../data/data_task/r10_supply_ele.parquet')
        .pivot(
            index=['model','scenario','region','unit','year'],
            columns='variable',
            values='value'
        )
        .drop_nulls(subset=['Secondary Energy|Electricity|Solar','Secondary Energy|Electricity|Wind'])
        .rename({
            'Secondary Energy|Electricity':'ele',
            'Secondary Energy|Electricity|Solar':'solar',
            'Secondary Energy|Electricity|Solar|PV':'pv',
            'Secondary Energy|Electricity|Solar|CSP':'csp',
            'Secondary Energy|Electricity|Wind':'wind',
            'Secondary Energy|Electricity|Wind|Onshore':'onshore',
            'Secondary Energy|Electricity|Wind|Offshore':'offshore',
        })
        .with_columns(
            pv=pl.when(pl.col('pv').is_null())
            .then(pl.col('solar'))
            .otherwise(pl.col('pv'))
        )
        .with_columns(
            onshore=pl.when(pl.col('onshore').is_null())
            .then(pl.col('wind'))
            .otherwise(pl.col('onshore'))
        )
        .with_columns(
            csp=pl.when(pl.col('csp').is_null())
            .then(
                pl.max_horizontal((pl.col('solar') - pl.col('pv')), 0)
            )
            .otherwise(pl.col('csp'))
        )
        .with_columns(
            offshore=pl.when(pl.col('offshore').is_null())
            .then(
                pl.max_horizontal((pl.col('wind') - pl.col('onshore')), 0)
            )
            .otherwise(pl.col('offshore'))
        )
        .with_columns(
            pv_base=pl.col('pv').sort_by('year').first().over('model','scenario','region'),
            csp_base=pl.col('csp').sort_by('year').first().over('model','scenario','region'),
            onshore_base=pl.col('onshore').sort_by('year').first().over('model','scenario','region'),
            offshore_base=pl.col('offshore').sort_by('year').first().over('model','scenario','region'),
        )
        .with_columns(
            pv_grow=pl.col('pv') - pl.col('pv_base'),
            csp_grow=pl.col('csp') - pl.col('csp_base'),
            onshore_grow=pl.col('onshore') - pl.col('onshore_base'),
            offshore_grow=pl.col('offshore') - pl.col('offshore_base'),
        )
        .with_columns(
            sum_grow=pl.col('pv_grow') + pl.col('csp_grow') + pl.col('onshore_grow') + pl.col('offshore_grow')
        )
        .with_columns(
            pv_share=pl.col('pv_grow') / pl.col('sum_grow'),
            csp_share=pl.col('csp_grow') / pl.col('sum_grow'),
            onshore_share=pl.col('onshore_grow') / pl.col('sum_grow'),
            offshore_share=pl.col('offshore_grow') / pl.col('sum_grow'),
        )
)

ele_gen.select(pl.col('model','scenario','region','year','pv_share','csp_share','onshore_share','offshore_share','pv_grow','csp_grow','onshore_grow','offshore_grow','sum_grow','pv','pv_base','csp','csp_base','onshore','onshore_base','offshore','offshore_base','solar','wind','ele','unit')).write_parquet('../data/data_task/r10_abate_ratio_by_ele.parquet')
