import pyodbc


############################# STAGING ###########################
##################### INCOME STATEMENT ########################

def create_stg_income_statement(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        DROP TABLE IF EXISTS [dbo].[{table_name}]

        CREATE TABLE [dbo].[{table_name}](
            [ID_COMPANY] [varchar](30) NULL,
            [Date] [varchar](30) NULL,
            [Revenue] [varchar](30) NULL,
            [Cost Of Goods Sold] [varchar](30) NULL,
            [Gross Profit] [varchar](30) NULL,
            [Research And Development Expenses] [varchar](30) NULL,
            [SG&A Expenses] [varchar](30) NULL,
            [Other Operating Income Or Expenses] [varchar](30) NULL,
            [Operating Expenses] [varchar](30) NULL,
            [Operating Income] [varchar](30) NULL,
            [Total Non-Operating Income/Expense] [varchar](30) NULL,
            [Pre-Tax Income] [varchar](30) NULL,
            [Income Taxes] [varchar](30) NULL,
            [Income After Taxes] [varchar](30) NULL,
            [Other Income] [varchar](30) NULL,
            [Income From Continuous Operations] [varchar](30) NULL,
            [Income From Discontinued Operations] [varchar](30) NULL,
            [Net Income] [varchar](30) NULL,
            [EBITDA] [varchar](30) NULL,
            [EBIT] [varchar](30) NULL,
            [Basic Shares Outstanding] [varchar](30) NULL,
            [Shares Outstanding] [varchar](30) NULL,
            [Basic EPS] [varchar](30) NULL,
            [EPS - Earnings Per Share] [varchar](30) NULL,
            [Industry] [nvarchar](50) NULL,
            [Sector] [varchar](30) NULL,
            [Company Name] [varchar](MAX) NULL,
            [Country] [varchar](30) NULL
        ) ON [PRIMARY]""")
    
    conn.commit()   

    return

def load_stg_income_statement(conn, concat_financial):
    cursor = conn.cursor()

    for i,row in concat_financial.iterrows():
        cursor.execute('''

            INSERT INTO dbo.FINANCIAL ([ID_COMPANY]
                                    ,[Date]
                                    ,[Revenue]
                                    ,[Cost Of Goods Sold]
                                    ,[Gross Profit]
                                    ,[Research And Development Expenses]
                                    ,[SG&A Expenses]
                                    ,[Other Operating Income Or Expenses]
                                    ,[Operating Expenses]
                                    ,[Operating Income]
                                    ,[Total Non-Operating Income/Expense]
                                    ,[Pre-Tax Income]
                                    ,[Income Taxes]
                                    ,[Income After Taxes]
                                    ,[Other Income]
                                    ,[Income From Continuous Operations]
                                    ,[Income From Discontinued Operations]
                                    ,[Net Income]
                                    ,[EBITDA]
                                    ,[EBIT]
                                    ,[Basic Shares Outstanding]
                                    ,[Shares Outstanding]
                                    ,[Basic EPS]
                                    ,[EPS - Earnings Per Share]
                                    ,[Industry]
                                    ,[Sector]
                                    ,[Company Name]
                                    ,[Country]
                                    )
                                    
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', 
                    
            tuple(row[['ticker', 'Unnamed: 0','Revenue', 'Cost Of Goods Sold',
                            'Gross Profit', 'Research And Development Expenses', 'SG&A Expenses',
                            'Other Operating Income Or Expenses', 'Operating Expenses',
                            'Operating Income', 'Total Non-Operating Income/Expense',
                            'Pre-Tax Income', 'Income Taxes', 'Income After Taxes', 'Other Income',
                            'Income From Continuous Operations',
                            'Income From Discontinued Operations', 'Net Income', 'EBITDA', 'EBIT',
                            'Basic Shares Outstanding', 'Shares Outstanding', 'Basic EPS',
                            'EPS - Earnings Per Share','industry', 'sector', 
                            'company_full_name', 'country']].values))
        
        conn.commit()
    
    return

##################### CASH FLOW STATEMENT ########################

def create_stg_cash_flow_statement(conn, table_name):

    cursor = conn.cursor()
    cursor.execute(f"""DROP TABLE IF EXISTS [dbo].[{table_name}]

                    CREATE TABLE [dbo].[{table_name}](
                    
                        [ID_COMPANY] [varchar](30) NULL,
                        [Date] [varchar](30) NULL,
                        
                        [Net Income/Loss] [varchar](30) NULL,
                        [Total Depreciation And Amortization - Cash Flow] [varchar](30) NULL,
                        [Other Non-Cash Items] [varchar](30) NULL,
                        [Total Non-Cash Items] [varchar](30) NULL,
                        [Change In Accounts Receivable] [varchar](30) NULL,
                        [Change In Inventories] [varchar](30) NULL,
                        [Change In Accounts Payable] [varchar](30) NULL,
                        [Change In Assets/Liabilities] [varchar](30) NULL,
                        [Total Change In Assets/Liabilities] [varchar](30) NULL,
                        [Cash Flow From Operating Activities] [varchar](30) NULL,
                        [Net Change In Property, Plant, And Equipment] [varchar](30) NULL,
                        [Net Change In Intangible Assets] [varchar](30) NULL,
                        [Net Acquisitions/Divestitures] [varchar](30) NULL,
                        [Net Change In Short-term Investments] [varchar](30) NULL,
                        [Net Change In Long-Term Investments] [varchar](30) NULL,
                        [Net Change In Investments - Total] [varchar](30) NULL,
                        [Investing Activities - Other] [varchar](30) NULL,
                        [Cash Flow From Investing Activities] [varchar](30) NULL,
                        [Net Long-Term Debt] [varchar](30) NULL,
                        [Net Current Debt] [varchar](30) NULL,
                        [Debt Issuance/Retirement Net - Total] [varchar](30) NULL,
                        [Net Common Equity Issued/Repurchased] [varchar](30) NULL,
                        [Net Total Equity Issued/Repurchased] [varchar](30) NULL,
                        [Total Common And Preferred Stock Dividends Paid] [varchar](30) NULL,
                        [Financial Activities - Other] [varchar](30) NULL,
                        [Cash Flow From Financial Activities] [varchar](30) NULL,
                        [Net Cash Flow] [varchar](30) NULL,
                        [Stock-Based Compensation] [varchar](30) NULL,
                        [Common Stock Dividends Paid] [varchar](30) NULL

                    ) ON [PRIMARY]""")
    conn.commit()  

    return


def load_stg_cash_flow_statement(conn, concat_cash):
    cursor = conn.cursor()

    for i,row in concat_cash.iterrows():
        cursor.execute('''

        INSERT INTO dbo.CASH (
        
        [ID_COMPANY],
        [Date],
        [Net Income/Loss],
        [Total Depreciation And Amortization - Cash Flow],
        [Other Non-Cash Items],
        [Total Non-Cash Items],
        [Change In Accounts Receivable],
        [Change In Inventories],
        [Change In Accounts Payable], 
        [Change In Assets/Liabilities],
        [Total Change In Assets/Liabilities],
        [Cash Flow From Operating Activities],
        [Net Change In Property, Plant, And Equipment],
        [Net Change In Intangible Assets],
        [Net Acquisitions/Divestitures],
        [Net Change In Short-term Investments],
        [Net Change In Long-Term Investments],
        [Net Change In Investments - Total],
        [Investing Activities - Other],
        [Cash Flow From Investing Activities],
        [Net Long-Term Debt],
        [Net Current Debt],
        [Debt Issuance/Retirement Net - Total],
        [Net Common Equity Issued/Repurchased],
        [Net Total Equity Issued/Repurchased],
        [Total Common And Preferred Stock Dividends Paid],
        [Financial Activities - Other],
        [Cash Flow From Financial Activities],
        [Net Cash Flow],
        [Stock-Based Compensation],
        [Common Stock Dividends Paid]
        
        )
                                
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', 
                    
        tuple(row[['ticker', 'Unnamed: 0',
                    
                    'Net Income/Loss',
                    'Total Depreciation And Amortization - Cash Flow',
                    'Other Non-Cash Items', 'Total Non-Cash Items',
                    'Change In Accounts Receivable', 'Change In Inventories',
                    'Change In Accounts Payable', 'Change In Assets/Liabilities',
                    'Total Change In Assets/Liabilities',
                    'Cash Flow From Operating Activities',
                    'Net Change In Property, Plant, And Equipment',
                    'Net Change In Intangible Assets', 'Net Acquisitions/Divestitures',
                    'Net Change In Short-term Investments',
                    'Net Change In Long-Term Investments',
                    'Net Change In Investments - Total', 'Investing Activities - Other',
                    'Cash Flow From Investing Activities', 'Net Long-Term Debt',
                    'Net Current Debt', 'Debt Issuance/Retirement Net - Total',
                    'Net Common Equity Issued/Repurchased',
                    'Net Total Equity Issued/Repurchased',
                    'Total Common And Preferred Stock Dividends Paid',
                    'Financial Activities - Other', 'Cash Flow From Financial Activities',
                    'Net Cash Flow', 'Stock-Based Compensation',
                    'Common Stock Dividends Paid']].values))
        
        conn.commit()

        return
    
##################### BALANCE SHEET STATEMENT ########################

def create_stg_balance_sheet_statement(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"""DROP TABLE IF EXISTS [dbo].[{table_name}]

                    CREATE TABLE [dbo].[{table_name}](
                        [ID_COMPANY] [varchar](30) NULL,
                        [Date] [varchar](30) NULL,
                        [Cash On Hand] [varchar](30) NULL,
                        [Receivables] [varchar](30) NULL,
                        [Inventory] [varchar](30) NULL,
                        [Pre-Paid Expenses] [varchar](30) NULL,
                        [Other Current Assets] [varchar](30) NULL,
                        [Total Current Assets] [varchar](30) NULL,
                        [Property, Plant, And Equipment] [varchar](30) NULL,
                        [Long-Term Investments] [varchar](30) NULL,
                        [Goodwill And Intangible Assets] [varchar](30) NULL,
                        [Other Long-Term Assets] [varchar](30) NULL,
                        [Total Long-Term Assets] [varchar](30) NULL,
                        [Total Assets] [varchar](30) NULL,
                        [Total Current Liabilities] [varchar](30) NULL,
                        [Long Term Debt] [varchar](30) NULL,
                        [Other Non-Current Liabilities] [varchar](30) NULL,
                        [Total Long Term Liabilities] [varchar](30) NULL,
                        [Total Liabilities] [varchar](30) NULL,
                        [Common Stock Net] [varchar](30) NULL,
                        [Retained Earnings (Accumulated Deficit)] [varchar](30) NULL,
                        [Comprehensive Income] [varchar](30) NULL,
                        [Other Share Holders Equity] [varchar](30) NULL,
                        [Share Holder Equity] [varchar](30) NULL,
                        [Total Liabilities And Share Holders Equity] [varchar](30) NULL,


                    ) ON [PRIMARY]""")
    conn.commit()  
    return

def load_stg_balance_sheet_statement(conn, concat_balance):
    cursor = conn.cursor()

    for i,row in concat_balance.iterrows():
        cursor.execute('''

                        INSERT INTO dbo.BALANCE (
                        
                        [ID_COMPANY],
                        [Date],
                        [Cash On Hand],
                        [Receivables],
                        [Inventory],
                        [Pre-Paid Expenses],
                        [Other Current Assets],
                        [Total Current Assets],
                        [Property, Plant, And Equipment],
                        [Long-Term Investments],
                        [Goodwill And Intangible Assets],
                        [Other Long-Term Assets],
                        [Total Long-Term Assets],
                        [Total Assets],
                        [Total Current Liabilities],
                        [Long Term Debt],
                        [Other Non-Current Liabilities],
                        [Total Long Term Liabilities],
                        [Total Liabilities],
                        [Common Stock Net],
                        [Retained Earnings (Accumulated Deficit)],
                        [Comprehensive Income],
                        [Other Share Holders Equity],
                        [Share Holder Equity],
                        [Total Liabilities And Share Holders Equity]                   
                        
                        
                        )
                                                
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', 
                    
                    tuple(row[['ticker', 'Unnamed: 0','Cash On Hand', 'Receivables',
                                'Inventory', 'Pre-Paid Expenses', 'Other Current Assets',
                                'Total Current Assets', 'Property, Plant, And Equipment',
                                'Long-Term Investments', 'Goodwill And Intangible Assets',
                                'Other Long-Term Assets', 'Total Long-Term Assets', 'Total Assets',
                                'Total Current Liabilities', 'Long Term Debt',
                                'Other Non-Current Liabilities', 'Total Long Term Liabilities',
                                'Total Liabilities', 'Common Stock Net',
                                'Retained Earnings (Accumulated Deficit)', 'Comprehensive Income',
                                'Other Share Holders Equity', 'Share Holder Equity',
                                'Total Liabilities And Share Holders Equity']].values))
        
        conn.commit()
        return

##################### FINANCIAL RATIOS ########################

def create_stg_financial_ratios(conn, table_name):
    cursor = conn.cursor()

    cursor.execute(f"""DROP TABLE IF EXISTS [dbo].[{table_name}]

                    CREATE TABLE [dbo].[{table_name}](
                        [ID_COMPANY] [varchar](30) NULL,
                        [Date] [varchar](30) NULL,
                        [Current Ratio] [varchar](30) NULL,
                        [Long-term Debt / Capital] [varchar](30) NULL,
                        [Debt/Equity Ratio] [varchar](30) NULL,
                        [Gross Margin] [varchar](30) NULL,
                        [Operating Margin] [varchar](30) NULL,
                        [EBIT Margin] [varchar](30) NULL,
                        [EBITDA Margin] [varchar](30) NULL,
                        [Pre-Tax Profit Margin] [varchar](30) NULL,
                        [Net Profit Margin] [varchar](30) NULL,
                        [Asset Turnover] [varchar](30) NULL,
                        [Inventory Turnover Ratio] [varchar](30) NULL,
                        [Receiveable Turnover] [varchar](30) NULL,
                        [Days Sales In Receivables] [varchar](30) NULL,
                        [ROE - Return On Equity] [varchar](30) NULL,
                        [Return On Tangible Equity] [varchar](30) NULL,
                        [ROA - Return On Assets] [varchar](30) NULL,
                        [ROI - Return On Investment] [varchar](30) NULL,
                        [Book Value Per Share] [varchar](30) NULL,
                        [Operating Cash Flow Per Share] [varchar](30) NULL,
                        [Free Cash Flow Per Share] [varchar](30) NULL
                    ) ON [PRIMARY]""")
    conn.commit()      
    return


##################### PRICES ########################

def create_stg_prcies(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(
        f"""
        DROP TABLE IF EXISTS [dbo].[{table_name}]

        CREATE TABLE [dbo].[{table_name}](
            [ID_COMPANY] [varchar](30) NULL,
            [Year] [varchar](30) NULL,
            [Longevity] [varchar](30) NULL,
            [Close] [varchar](30) NULL,
            [Growth -1] [varchar](30) NULL,
            [Growth +1] [varchar](30) NULL,
            [Growth +5] [varchar](30) NULL,
            [avgGrowth -10] [varchar](30) NULL,
            [avgGrowth -5] [varchar](30) NULL
            
        ) ON [PRIMARY]
        """
                    )
    conn.commit()  
    return