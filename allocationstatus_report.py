import pandas as pd
import MySQLdb
con=MySQLdb.connect("172.16.0.55","root","admin123*","rackanalyticsdb")
sql="""select distinct
en_terminal_name,
supplier_terminal_name,
supplier_name,
en_account_type,
en_branding,
Account_Type,
Product_Category,
product_type,
product_name,
period ,
execution_date,
date(execution_Date) as executiondate,
batchno,
en_allocation_status
from
rackanalyticsdb.pilot_15days

where
supplier_name = "Holly" """
df=pd.read_sql(sql,con)
print "Got Frame"
df["dt"]=df[u'executiondate'].apply(lambda x:str(x))+"_"+df[u'batchno'].apply(lambda x:str(x).strip())
pv3=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'],columns=["dt"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
# pv=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period', u'execution_date', u'batchno'],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
grp=df.groupby([u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'])
df["status"]=None
for k,g in grp:
    stat=len(g["en_allocation_status"].drop_duplicates())
    message="No" if stat ==1 else "Yes"
    df.loc[g.index,"status"]=message
p1=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period','status'],columns=["dt"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
pv3.to_csv("en_stat.csv")
p1.to_csv("en_allocation_status_report.csv")
# pv3.to_excel("testexcel.xls")
con.close()
# df["poll"]=None
 # for k,g in grp:
 #     t=g["execution_date"].drop_duplicates()
 #     print t
 #     c=1
 #     for i in t:
 #         df.loc[df["execution_date"]==i,"poll"]=c
 #         c=c+1
 # grp=df.groupby(["supplier_terminal_name","supplier_name","Account_Type","product_name"])
 # pv1=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'],columns=["poll"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
# pv1.to_csv("sample1.csv")

# pv2=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period', u'execution_date', u'batchno'],columns=["executiondate"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
# pv2.to_csv("sample2.csv")
# pv2.to_excel("t.xls")