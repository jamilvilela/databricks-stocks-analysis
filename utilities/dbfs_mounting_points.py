# Databricks notebook source
# MAGIC %run ../utilities/parameters

# COMMAND ----------

def mounting_point(tier: str):

    prm = Parameters()
    params = prm.get_params()

    ACCESS_KEY = params['ACCESS_KEY']
    SECRET_KEY = params['SECRET_KEY']

    bucket_name = f'BUCKET_{tier}'
    source = f"{params['BUCKET_PREFIX']}{ACCESS_KEY}:{SECRET_KEY}@{params[bucket_name]}"
    mount_name = f"/mnt/{params[bucket_name]}"

    print(mount_name)

    existing_mounts = [mount.mountPoint for mount in dbutils.fs.mounts()]
    if mount_name in existing_mounts:
        print('Already mounted: ', mount_name)
    else:
        print('Mounting: ', mount_name)
        dbutils.fs.mount(source, mount_name)

    return

# COMMAND ----------


layers = ['BRONZE', 'SILVER', 'GOLD']
for layer in layers:
    mounting_point(layer)


# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls('/mnt/jamil-lakehouse-gold/stocks-analysis/'))

# COMMAND ----------


