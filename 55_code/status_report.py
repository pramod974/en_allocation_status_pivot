import pandas as pd
import MySQLdb
import copy
class report:
    def __init__(self):
        self.con=MySQLdb.connect("172.16.0.55","root","admin123*","rackanalyticsdb")
    def __del__(self):
        self.con.close()
    def parse_supplier_all(self):
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
        en_allocation_status,
        terminal_name as polls
        from
        rackanalyticsdb.pilot_15days

        where
        en_terminal_name <> "unknown"
        and
        supplier_terminal_name <> "unknown"
        and
        en_account_type <> "unknown" and en_account_type <> "None"
        and
        en_branding <> "unknown" and en_branding <> "None"
        and
        Account_Type <> "unknown"
         and
        Product_Category <> "unknown"
         and
         product_type  <> "unknown"
         and
         product_name <> "unknown"
         and
         period <> "unknown"
         and
         en_allocation_status <> "unknown"
        """
        df=pd.read_sql(sql,self.con)
        print "Got Frame"
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
        print "Parsing Groups"
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
                print "****************Completed*************************"
                print k
                print "___________________________________________________"
            except Exception as e:
                print e
        # p6=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period','status'],fill_value="",columns=["polls"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        # print dataRow
        f=open("en_allocation_status_Report1.csv","w")
        f.write(header)
        for i in dataRow:
            f.write(i+"\n")
        f.close()

re=report()
# re.parse_supplier("BP")
re.parse_supplier_all()