import pandas as pd
import copy
df=pd.read_csv(r"C:\Users\pramod.kumar\Documents\en_allocation_report\original_frame.csv")
# df=df[df.batchno<=20]
header='en_terminal_name~supplier_terminal_name~supplier_name~en_account_type~en_branding~Account_Type~Product_Category~product_type~product_name~period'+"~Allocation_Status_Change~"

grp=df.groupby([u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'])
distinctDates=df["polls"].drop_duplicates().tolist()
distinctDates.sort()
dates={}
for i in distinctDates:
    dates[i]=''
# df["status"]=None
header=header+'~'.join(dates.keys())+"\n"
dataRow=[]
for k,g in grp:
    try:
        daterow=copy.deepcopy(dates)
        stat=len(g["en_allocation_status"].drop_duplicates())
        message="No" if stat ==1 else "Yes"
        # df.loc[g.index,"status"]=message
        for en,p in zip(g["en_allocation_status"],g["polls"]):
            # print en,p
            daterow[p]=en
        row='~'.join(k)+"~"+message+"~"+'~'.join(daterow.values())
        dataRow.append(row)
    except Exception as e:
        print e
# p6=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period','status'],fill_value="",columns=["polls"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
# print dataRow
f=open("en_allocation_status_Report6.csv","w")
f.write(header)
for i in dataRow:
    f.write(i+"\n")
f.close()
