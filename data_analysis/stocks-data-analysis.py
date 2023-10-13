# Databricks notebook source
# MAGIC %md
# MAGIC ### Dashboard notebook

# COMMAND ----------

import pyspark.sql.functions as F 
from datetime import date, datetime, timedelta

start_time = datetime.now()

# COMMAND ----------

# MAGIC
# MAGIC %run ../utilities/elt_utils
# MAGIC

# COMMAND ----------

util = elt_util()

ticker_list = util.get_wallets_tickers(filter={'stocks.type': 'Stock'})
ticker_list.sort()
ticker = ticker_list[0]

dbutils.widgets.dropdown('Period', 'Annual', ['Annual', 'Quarterly'])
dbutils.widgets.dropdown('Ticker', ticker, ticker_list)

ticker = dbutils.widgets.get('Ticker')
Period = dbutils.widgets.get('Period')
Period = 'Quarter' if Period == 'Quarterly' else Period


# COMMAND ----------


net_income = 'Net Income'

price_df = spark.sql(f"""select 
                            prc.ticker,
                            prc.year_month,
                            prc.vl_open, 
                            prc.vl_close,
                            prc.vl_moving_avg_200_days,
                            i.income_value 
                        from (
                            select 
                                p.ticker, 
                                concat_ws('-', c.dt_year, c.dt_month) year_month, 
                                first_value(p.vl_open) over(partition by c.dt_year, c.dt_month  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as vl_open,  
                                last_value(p.vl_close) over(partition by c.dt_year, c.dt_month  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as vl_close, 
                                last_value(p.vl_moving_avg_200_days) over (partition by c.dt_year, c.dt_month  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) vl_moving_avg_200_days
                            from 
                                stocks_analysis.prices p
                            inner join 
                                stocks_analysis.calendar c on c.dt_date = p.price_date
                            where 
                                p.ticker = '{ticker}'
                                and p.price_date >= (current_date() - INTERVAL 120 MONTH)
                        ) prc
                        left join
                            stocks_analysis.income i on prc.year_month = concat_ws('-', year(i.income_report_date), lpad(month(i.income_report_date), 2, 0)) 
                                                    and prc.ticker = i.ticker
                                                    and i.income_description = '{net_income}' 
                                                    and i.income_period = '{Period}'
                        group by 
                            prc.ticker, 
                            prc.year_month,
                            prc.vl_open ,  
                            prc.vl_close, 
                            prc.vl_moving_avg_200_days,
                            i.income_value
                        order by 
                            prc.ticker, prc.year_month
                     """)

price_df.display()

# COMMAND ----------


dividend_df = spark.sql(f"""
                            with dividend_year as (
                                select 
                                    d.ticker, 
                                    year(d.ex_date) dt_year,
                                    sum(d.vl_dividends) vl_dividends
                                from 
                                    stocks_analysis.dividends d
                                where 
                                    ticker = '{ticker}' 
                                    and d.ex_date >= '2014' --(current_date() - INTERVAL 9 YEAR)
                                group by                            
                                    d.ticker, 
                                    year(d.ex_date)
                            )
                            select
                                ticker, 
                                dt_year,
                                round(vl_dividends,4) vl_dividends,
                                round(lag(vl_dividends, 1) over (partition by ticker order by dt_year), 4) vl_dividend_prev,
                                round((vl_dividends - vl_dividend_prev) / vl_dividend_prev * 100, 2) vl_dividend_growth
                            from 
                                dividend_year                            
                        """)

dividend_df.display()

# COMMAND ----------

# DBTITLE 1,Income and Profit

net_income = 'Net Income'
revenue = 'Total Revenue'

income_df = spark.sql(f"""
                        select
                            ticker, 
                            income_period, 
                            case when i.income_period = 'Annual' then c.dt_year else concat(c.dt_year,  c.dt_quarter) end as dt,
                            case when i.income_period = 'Annual' then c.dt_year else c.dt_quarter_long end as dt_period,
                            sum(case when i.income_description = '{net_income}' then i.income_value end) as Net_Income,
                            sum(case when i.income_description = '{revenue}' then i.income_value end) as Total_Revenue,
                            round(Net_Income * 100 / Total_Revenue, 2)  as Profit_Margin
                        from
                            stocks_analysis.income i
                        inner join 
                            stocks_analysis.calendar c on c.dt_date = i.income_report_date 
                        where
                            i.ticker = '{ticker}'
                            and (i.income_description = '{net_income}' or i.income_description = '{revenue}')
                            and i.income_period = '{Period}'
                        group by
                                i.ticker, 
                                i.income_period,
                                case when i.income_period = 'Annual' then c.dt_year else concat(c.dt_year,  c.dt_quarter) end,
                                case when i.income_period = 'Annual' then c.dt_year else c.dt_quarter_long end 
                        order by 
                                case when i.income_period = 'Annual' then c.dt_year else concat(c.dt_year,  c.dt_quarter) end
                    """)

income_df.display()


# COMMAND ----------


total_assets = 'Total Assets'
total_debts = 'Total Liabilities Net Minority Interest'

balance_df = spark.sql(f"""
                        select
                            ticker, 
                            balance_period, 
                            case when b.balance_period = 'Annual' then c.dt_year else concat(c.dt_year,  c.dt_quarter) end as dt,
                            case when balance_period = 'Annual' then c.dt_year else c.dt_quarter_long end as dt_period,
                            sum(case when balance_description = '{total_assets}' then balance_value end) as Total_Assets,
                            sum(case when balance_description = '{total_debts}' then balance_value end) as Total_Liabilities,
                            round(Total_Liabilities * 100 / Total_Assets, 2)  as Debt_to_Assets
                        from
                            stocks_analysis.balance b
                        inner join 
                            stocks_analysis.calendar c on c.dt_date = b.balance_report_date 
                        where
                            ticker = '{ticker}'
                            and (balance_description = '{total_assets}' or balance_description = '{total_debts}')
                            and balance_period = '{Period}'
                        group by
                                ticker, 
                                balance_period, 
                                case when b.balance_period = 'Annual' then c.dt_year else concat(c.dt_year,  c.dt_quarter) end,
                                case when b.balance_period = 'Annual' then c.dt_year else c.dt_quarter_long end 
                        order by 
                                case when b.balance_period = 'Annual' then c.dt_year else concat(c.dt_year,  c.dt_quarter) end
                    """)

balance_df.display()


# COMMAND ----------

change_df = spark.sql(f"""
                        with qry_max_date as (
                            select 
                                _p.ticker,
                                max(_p.price_date) max_date
                            from 
                                stocks_analysis.prices _p
                            where 
                                _p.ticker = '{ticker}'
                            group by all
                        )
                        select 
                            p.ticker, 
                            p.price_date,
                            p.vl_close, 
                            c.dt_day_of_week_full, 
                            c.dt_day,
                            c.dt_month_full,
                            c.dt_year,
                            c.dt_full_date,
                            p.perc_change_day, 
                            p.perc_change_month, 
                            p.perc_change_week,
                            p.vl_change_day,
                            p.vl_change_week, 
                            p.vl_change_month,
                            p.vl_moving_avg_200_days
                        from 
                            stocks_analysis.prices p
                        inner join 
                            stocks_analysis.calendar c on p.price_date = c.dt_date
                        inner join
                            qry_max_date m on m.max_date = p.price_date
                        where 
                            p.ticker = '{ticker}'
                    """)


# COMMAND ----------

change_df.select('ticker', 'dt_full_date', 'perc_change_day').display()

# COMMAND ----------

change_df.select('ticker', 'dt_month_full', 'perc_change_month').display()

# COMMAND ----------

change_df.select('ticker', 'dt_year', 'vl_close', 'vl_moving_avg_200_days').display()

# COMMAND ----------


first_date = '2014-01-01'

rates_df = spark.sql(f"""
                        select
                            s.rate_type,
                            s.rate_date, 
                            s.rate_value
                        from
                            stocks_analysis.selic s
                        inner join 
                            stocks_analysis.calendar c on c.dt_date = s.rate_date                         
                        where
                            s.rate_date >= '{first_date}'
                        union all
                        select
                            e.rate_type,
                            e.rate_date, 
                            e.rate_value
                        from
                            stocks_analysis.effr e
                        inner join 
                            stocks_analysis.calendar c on c.dt_date = e.rate_date 
                        where
                            e.rate_date >= '{first_date}'
                    """)

rates_df.display()


# COMMAND ----------


