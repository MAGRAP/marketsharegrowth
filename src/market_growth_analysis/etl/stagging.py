import pyodbc


############################# STAGING ###########################
##################### INCOME STATEMENT ########################

def create_stg_income_statement(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        DROP TABLE IF EXISTS [dbo].[FINANCIAL]

        CREATE TABLE [dbo].[FINANCIAL](
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

def create_stg_cash_flow_statement(conn):

    cursor = conn.cursor()
    cursor.execute("""DROP TABLE IF EXISTS [dbo].[CASH]

                    CREATE TABLE [dbo].[CASH](
                    
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
