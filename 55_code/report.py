import pandas as pd
import MySQLdb

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
        supplier_name <> "holly"
        and
        supplier_name<> "shell"
        and
        en_terminal_name <> "0unknown"
        and
        supplier_terminal_name <> "unknown"
        and
        Account_Type <> "unknown"
         and
         product_name <> "unknown"
         and
         period <> "unknown"
         and
         en_allocation_status <> "unknown"
        """
        df=pd.read_sql(sql,self.con)
        print "Got Frame"
        # df["polls"]=df[u'executiondate'].apply(lambda x:str(x))+"_"+df[u'batchno'].apply(lambda x:str(x).strip())
        # pv3=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'],columns=["dt"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        # pv=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period', u'execution_date', u'batchno'],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        grp=df.groupby([u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'])
        df["status"]=None
        for k,g in grp:
            row = ','.join(k)
            for grpRow in g:
                grpRow
            stat=len(g["en_allocation_status"].drop_duplicates())
            message="No" if stat ==1 else "Yes"
            df.loc[g.index,"status"]=message

        p1=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period','status'],columns=["polls"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        # pv3.to_csv("en_allocation_status_report.csv")
        p1.to_csv("en_allocation_status_report1.csv")
    def parse_supplier(self,supplier):
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
        supplier_name='%s'
        and
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
        """%(supplier)
        df=pd.read_sql(sql,self.con)
        print "Got Frame"
        df["polls"]=df[u'executiondate'].apply(lambda x:str(x))+"_"+df[u'batchno'].apply(lambda x:str(x).strip())
        # pv3=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'],columns=["dt"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        # pv=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period', u'execution_date', u'batchno'],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        grp=df.groupby([u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period'])
        df["status"]=None
        for k,g in grp:
            stat=len(g["en_allocation_status"].drop_duplicates())
            message="No" if stat ==1 else "Yes"
            df.loc[g.index,"status"]=message
        p1=pd.pivot_table(df,index=[u'en_terminal_name', u'supplier_terminal_name', u'supplier_name', u'en_account_type', u'en_branding', u'Account_Type', u'Product_Category', u'product_type', u'product_name', u'period','status'],fill_value="",columns=["polls"],values= [ u'en_allocation_status'],aggfunc=lambda x :x)
        # pv3.to_csv("en_allocation_status_report.csv")
        p1.to_csv(supplier+"_en_allocation_status_report1.csv")
        # pv3.to_excel("testexcel.xls")

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
    def get_suppliers(self):
        suppliers_sql="""select distinct supplier_name from pilot_15days where supplier_name <> "valero" and supplier_name <> "murphy";"""
        cur=self.con.cursor()
        cur.execute(suppliers_sql)
        suppliers=cur.fetchall()
        for supplier in suppliers:
            try:
                self.parse_supplier(supplier[0])
            except Exception as e:
                print e

re=report()
# re.parse_supplier("BP")
re.get_suppliers()