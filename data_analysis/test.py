# Databricks notebook source
phones = [('Jack', 1234), ('Lena', 3333), ('Mark', 9999), ('Anna', 7582)]
header = ['name', 'phone_number']

calls = [(25, 1234, 7582, 8),(7, 9999, 7582, 1),(18, 9999, 3333, 4),(2, 7582, 3333, 3),(3, 3333, 1234, 1),(21, 3333, 1234, 1)]
fields = ['id', 'caller', 'callee', 'duration']

phones_df = spark.createDataFrame(phones, header)
phones_df.createOrReplaceTempView('phones')

calls_df = spark.createDataFrame(calls, fields)
calls_df.createOrReplaceTempView('calls')

calls_df.display()

# COMMAND ----------


promo_df = spark.sql(f"""
                        with 
                            duration as (
                                SELECT caller as phone, duration
                                FROM calls
                                union all
                                select callee as phone, duration
                                from calls 
                            ),
                            grouped as (
                                select name, phone, sum(duration) duration
                                from phones p
                                inner join duration d on d.phone = p.phone_number
                                group by name, phone
                                having sum(duration) >=10
                            )
                        select name
                        from grouped
                        order by name

                    """)

promo_df.display()            

# COMMAND ----------


