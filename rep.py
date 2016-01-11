import pandas as pd
df=pd.read_csv(r"G:\Projects\Rackinsight\en_allocation_status_pivot\status_frame.csv")
grp=df.groupby([u'supplier_terminal_name', u'supplier_name', u'Account_Type',u'product_name', u'period'])
df["status"]=None
for k,g in grp:
    stat=len(g["en_allocation_status"].drop_duplicates())
    message="No" if stat ==1 else "Yes"
    df.loc[g.index,"status"]=message
p6=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period','status'],fill_value="",columns=["polls"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)