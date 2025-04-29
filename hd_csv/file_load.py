import pandas as pd
[dev]
account = "zib58847.us-east-1"
user = "mk"
password = "MaBrian1234!@#$"
database = "NOM"
warehouse = "compute_WH"
role = "accountadmin"
df=pd.read_csv("hd2017.csv",encoding='ISO-8859-1') #UnicodeDecodeError: 'utf-8' codec can't decode
success,nchunks,nrows,_=write_pandas(dev, df, 'hd_data', auto_create_table=False, overwrite=True, quote_identifiers=False)
