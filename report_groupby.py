

import pandas as pd

"""
Psydo Code:

#NOTE OF EXISTING COMMON REPORT BUCKETS FROM THE VINTAGE ACQ REPORTS
        # VINTAGE
        # LN COUNT
        # UPB TOTAL
        # UPB AVERAGE
        # BRW FICO
        # CO-BWR FICO
        # LTV RATIO
        # CLTV RATIO
        # DTI
        # INTEREST RATE
        
To Do: Fix bug in the list comprehension version fo the wa loop. 
"""


class CollectCompiledData():


    def files_to_df(self, fname, filter_cols, filter_rows):
        """
        Collect merge file data, although the pd.read_csv() is general enough to be able to support
        any comma separate data.

        Not currently column or row filtered but could add filter of column data to improve performance

        Index also not currently overwritten, but could be done.

        unused arguments:

        # index = 'LOAN IDENTIFIER'
        # index_col = index
        #index_col=False,
        #usecols=filter_columns,
        #nrows = 1000

        """
        known_columns = ['LOAN IDENTIFIER', 'MONTHLY REPORTING PERIOD', 'SERVICER NAME',
                         'CURRENT INTEREST RATE', 'CURRENT ACTUAL UPB', 'LOAN AGE',
                         'REMAINING MONTHS TO LEGAL MATURITY', 'ADJUSTED MONTHS TO MATURITY',
                         'MATURITY DATE', 'METROPOLITAN STATISTICAL AREA (MSA)',
                         'CURRENT LOAN DELINQUENCY STATUS', 'MODIFICATION FLAG',
                         'ZERO BALANCE CODE', 'ZERO BALANCE EFFECTIVE DATE',
                         'LAST PAID INSTALLMENT DATE', 'FORECLOSURE DATE', 'DISPOSITION DATE',
                         'FORECLOSURE COSTS', 'PROPERTY PRESERVATION AND REPAIR COSTS',
                         'ASSET RECOVERY COSTS', 'MISCELLANEOUS HOLDING EXPENSES AND CREDITS',
                         'ASSOCIATED TAXES FOR HOLDING PROPERTY', 'NET SALE PROCEEDS',
                         'CREDIT ENHANCEMENT PROCEEDS', 'REPURCHASE MAKE WHOLE PROCEEDS',
                         'OTHER FORECLOSURE PROCEEDS', 'NON INTEREST BEARING UPB',
                         'PRINCIPALFORGIVENESS AMOUNT', 'REPURCHASE MAKE WHOLE PROCEEDS FLAG',
                         'FORECLOSURE PRINCIPAL WRITE-OFF AMOUNT',
                         'SERVICING ACTIVITY INDICATOR', 'SOURCE_x', 'ORIGINATION CHANNEL',
                         'SELLER NAME', 'ORIGINAL INTEREST RATE', 'ORIGINAL UPB',
                         'ORIGINAL LOAN TERM', 'ORIGINATION DATE', 'FIRST PAYMENT DATE',
                         'ORIGINAL LOAN-TO-VALUE (LTV)',
                         'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'NUMBER OF BORROWERS',
                         'ORIGINAL DEBT TO INCOME RATIO', 'BORROWER CREDIT SCORE AT ORIGINATION',
                         'FIRST TIME HOME BUYER INDICATOR', 'LOAN PURPOSE', 'PROPERTY TYPE',
                         'NUMBER OF UNITS', 'OCCUPANCY TYPE', 'PROPERTY STATE', 'ZIP CODE SHORT',
                         'PRIMARY MORTGAGE INSURANCE PERCENT', 'PRODUCT TYPE',
                         'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'MORTGAGE INSURANCE TYPE',
                         'RELOCATION MORTGAGE INDICATOR', 'SOURCE_y'] #just an FYI for this specific merged file


        df = pd.read_csv(fname, sep = ",", usecols=filter_cols, nrows=filter_rows,)
        # print(df.shape) #develop check
        drop = ['Unnamed: 0', 'Unnamed: 0.1', 'Unnamed: 0_x']
        df2 = df.drop(drop, axis=1)
        print(df2.shape)  #developer check, show progress of script exceution
        return df2

    def df_to_files(self, df, fname):
        """
        Very generic write out to csv
        :param df_name:
        :param fname:
        :return:
        """
        df.to_csv(fname, sep=',')
        return


class GroupBySummary():

    def create_group_dict(self, by_field, wa_field, wa_list, sum_list, avg_list, report_name):
        """
        previous development, to create vintage dict
        by_field = 'UPB_BUCKET'
        wa_field = 'ORIGINAL UPB'
        wa_list = [['Int Rate WA', 'ORIGINAL INTEREST RATE'], ['BRW FICO WA', 'BORROWER CREDIT SCORE AT ORIGINATION'],
               ['CO BRW FICO WA', 'CO-BORROWER CREDIT SCORE AT ORIGINATION'],
               ['LTV WA', 'ORIGINAL LOAN-TO-VALUE (LTV)'],
               ['CLTV WA', 'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)'], ['DTI WA', 'ORIGINAL DEBT TO INCOME RATIO']]
        sum_list = [['Total_UPB', 'ORIGINAL UPB'], ['Total_Loan_Count', 'LOAN_COUNT']]
        avg_list = [['Avg UPB','ORIGINAL UPB']]
        :param by_field:
        :param wa_field:
        :param wa_list:
        :param sum_list:
        :param avg_list:
        :return:
        """
        dict = {}
        dict['by_field'] = by_field
        dict['wa_field'] = wa_field
        dict['wa_list'] = wa_list
        dict['sum_list'] = sum_list
        dict['avg_list'] = avg_list
        dict['report_name'] = report_name
        print(dict)
        return dict

    def generate_summmary(self, df, group_dict):
        x = CollectCompiledData()
        y = GroupBySummary()
        try:
            by_field = group_dict['by_field']
            wa_field = group_dict['wa_field']
            wa_list = group_dict['wa_list']
            sum_list = group_dict['sum_list']
            avg_list = group_dict['avg_list']
            report_name = group_dict['report_name']
        except ValueError:
            print("Error 1: Issue with Report Dictionary")
        else:
            b = GroupBySummary.sum_group(y, df, sum_list, by_field)
            c = GroupBySummary.avg_group(y, df, avg_list, by_field)
            d = GroupBySummary.wa_group(y, df, wa_list, wa_field, by_field)
            print(d.shape)
            print(d)
            e = pd.concat([b, c, d], axis=1)
            print(e.shape)
            print(e)
            fname = ('draft_groupby_data/'+report_name+'.csv')
            f = CollectCompiledData.df_to_files(x, e, fname)
            return e

    def wa_iterate(self, df, wa_list, weight, by_field):
        y = GroupBySummary()  #invoke groupby class
        df1 = df  #collect dataframe from input
        dfs = []   #placeholder list for multiple data frames
        for l in wa_list:  #for the items in the weighted average list
            wa_result = l[0]  #collect the derived data column name
            df1[wa_result] = 0  #placholder value of zero, until later calculation done
            wa_field = l[1]  #collect the raw input file value
            df2 = GroupBySummary.wa_group(y, df1, wa_field, wa_result, weight, by_field)  #invoke method for weight average
            dfs.append(df2)  #append to the placeholder list
        df3 = pd.concat(dfs, axis=1, ignore_index=False, sort=True)  #convert the placeholder list into a dataframe
        return df3  #return consolidated dataframe


    def wa_group(self, df, wa_field, wa_result, weight, by_field):
        """
        list comprehension version, has a bug i believe, but leaving in to be fixed in future development
        original value for the weight_field was 'ORIGINAL UPB',
        the field by which the other fields are weighted for the average,
        in this case unpaid principal balance
        df['Int Rate WA'] = round((df['ORIGINAL INTEREST RATE'] * df[weight_field]).sum()/df[weight_field].sum(),1)
        original value for the drop fields:
        drop_2 = ['ORIGINAL INTEREST RATE','BORROWER CREDIT SCORE AT ORIGINATION','ORIGINAL UPB', 'VINTAGE','LOAN COUNT', 'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'ORIGINAL LOAN-TO-VALUE (LTV)','ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'ORIGINAL DEBT TO INCOME RATIO']
        removes all other fields subject to this wa_group calculation
        :param df:  input dataframe, raw from the import data
        :param wa_field: user specified fields/columns they would like to see weighted averaged
        :param wa_result: user specified fields/columns they would like to see weighted averaged
        :param weight:  user specified field to use calculate teh weighted average
        :param by_field: groupby reporting field, usually the tagging buckets made earlier in the process
        :return:  the groupby defined dataframe, apply average
        """
        cols = df.columns.tolist()  #collect list of all column fields
        cols.remove(by_field)   #remove field needed for calculation
        cols.remove(wa_result)  #remove field for calculation
        cols.remove(wa_field)  #remove field for calculation
        cols.remove(weight)  #remove field for calculation
        df1 = df.drop(cols, axis=1)  # get rid of all other columns you don't need, import for speed
        df1[wa_result] = [(df1.loc[i, wa_field] * df1.loc[i, weight]) / df1.loc[i, weight] for i in range(len(df1[wa_field]))]  #list comprehension to calculate the weighted average
        df2 = df1.drop(wa_field, axis=1)  # get rid of all other columns you don't need, import for speed
        df3 = df2.drop(weight, axis=1)  #remove unneeded fields
        df4 = df3.groupby(by_field)  #groupby executed to aggreate data
        print(df4)  #developer check monitor completness of the execution
        return df4.sum()  #sum data of the aggregated data on upon exit.

    def wa_iterate_loop(self, df, wa_list, weight, by_field):
        y = GroupBySummary()  #invoke groupby class
        df1 = df  #collect dataframe from input
        dfs = [] #placeholder list for multiple data frames
        for l in wa_list:
            wa_result = l[0]  #collect the derived data column name
            df1[wa_result] = 0  #placholder value of zero, until later calculation done
            wa_field = l[1]  #collect the raw input file value
            df2 = GroupBySummary.wa_group_loop(y, df1, wa_field, wa_result, weight, by_field)  #invoke method for weight average
            dfs.append(df2)  #append to the placeholder list
        df3 = pd.concat(dfs, axis=1, ignore_index=False, sort=True)  #convert the placeholder list into a dataframe
        return df3 #return consolidated dataframe

    def wa_group_loop(self, df, wa_field, wa_result, weight, by_field):
        """
        original value for the weight_field was 'ORIGINAL UPB',
        the field by which the other fields are weighted for the average,
        in this case unpaid principal balance
        df['Int Rate WA'] = round((df['ORIGINAL INTEREST RATE'] * df[weight_field]).sum()/df[weight_field].sum(),1)
        original value for the drop fields:
        drop_2 = ['ORIGINAL INTEREST RATE','BORROWER CREDIT SCORE AT ORIGINATION','ORIGINAL UPB', 'VINTAGE','LOAN COUNT', 'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'ORIGINAL LOAN-TO-VALUE (LTV)','ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'ORIGINAL DEBT TO INCOME RATIO']
        removes all other fields subject to this wa_group calculation
        :param df: input dataframe, raw from the import data
        :param weight_field:  user specified field to use calculate teh weighted average
        :param wa_list:  user specified fields/columns they would like to see weighted averaged
        :param by_field: groupby reporting field, usually the tagging buckets made earlier in the process
        :return:the groupby defined dataframe, apply average
        """
        for i in range(len(df[wa_field])):
            df.loc[i, wa_result] = (df.loc[i, wa_field] * df.loc[i, weight]) / df.loc[i, weight]
        cols = df.columns.tolist() #collect list of all column fields
        cols.remove(by_field)   #remove field for calculation
        cols.remove(wa_result)  #remove field for calculation
        df1 = df.drop(cols, axis=1)  #remove unneeded fields
        df2 = df1.groupby(by_field)  #groupby executed to aggreate data
        print(df2)  #developer check monitor completness of the execution
        return df2.mean()  #return consolidated dataframe

    def sum_group_loop(self, df, sum_list,by_field):
        """
        Same output as list comprehension but slower
        df['Total UPB'] = round(df['ORIGINAL UPB'],0)
        df['Loan Count'] = round(df['LOAN COUNT'],0)
        drop_2 = ['ORIGINAL INTEREST RATE', 'BORROWER CREDIT SCORE AT ORIGINATION', 'ORIGINAL UPB', 'VINTAGE', 'LOAN COUNT',
                  'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'ORIGINAL LOAN-TO-VALUE (LTV)',
                  'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'ORIGINAL DEBT TO INCOME RATIO']
        :param df:  input dataframe, raw from the import data
        :param sum_list:  user specified fields/columns they would like to arithmetically summed
        :param by_field:   groupby reporting field, usually the tagging buckets made earlier in the process
        :return:  the groupby defined dataframe, apply sum
        """
        cols = df.columns.tolist()  #collect list of all column fields
        cols.remove(by_field)  #remove field for calculation
        for l in sum_list: #for the fields in the sum_list
            df[l[0]] = round(df[l[1]], 0)  #copy them to the new field, round them
        df1 = df.drop(cols, axis=1) #get rid of all other columns you don't need, import for speed
        df2 = df1.groupby(by_field)  #groupby executed to aggreate data
        return df2.sum() #apply sum on the return

    def sum_iterate(self, df, sum_list, by_field):
        y = GroupBySummary() #invoke groupby class
        df1 = df  #collect dataframe from input
        dfs = []  #placeholder list for multiple data frames
        for l in sum_list:
            sum_result = l[0]  #collect the derived data column name
            df1[sum_result] = 0  #placholder value of zero, until later calculation done
            sum_field = l[1]  #collect the raw input file value
            df2 = GroupBySummary.sum_group(y, df1, sum_field, sum_result, by_field)  #invoke method for sum aggregation
            dfs.append(df2)  #append to the placeholder list
        df3 = pd.concat(dfs, axis=1, ignore_index=False, sort=True) #convert the placeholder list into a dataframe
        print(df3.head())  #developer check monitor completness of the execution
        return df3  #return consolidated dataframe

    def sum_group(self, df, sum_field , sum_result, by_field):
        """
        df['Total UPB'] = round(df['ORIGINAL UPB'],0)
        df['Loan Count'] = round(df['LOAN COUNT'],0)
        drop_2 = ['ORIGINAL INTEREST RATE', 'BORROWER CREDIT SCORE AT ORIGINATION', 'ORIGINAL UPB', 'VINTAGE', 'LOAN COUNT',
                  'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'ORIGINAL LOAN-TO-VALUE (LTV)',
                  'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'ORIGINAL DEBT TO INCOME RATIO']
        :param df:  input dataframe, raw from the import data
        :param sum_list:   user specified fields/columns they would like to arithmetically summed
        :param by_field:  groupby reporting field, usually the tagging buckets made earlier in the process
        :return: the groupby defined dataframe, apply sum
        """
        cols = df.columns.tolist()  #collect list of all column fields
        cols.remove(by_field)   #remove field for calculation
        cols.remove(sum_result)  #remove field for calculation
        cols.remove(sum_field)  #remove field for calculation
        df1 = df.drop(cols, axis=1)  # get rid of all other columns you don't need, import for speed
        df1[sum_result] = (round(df1[sum_field], 0))  #copy them to the new field, round them
        df2 = df1.drop(sum_field, axis=1) #get rid of all other columns you don't need, import for speed
        df3 = df2.groupby(by_field)  #groupby executed to aggreate data
        return df3.sum() #apply sum on the return

    def avg_iterate(self, df, avg_list, by_field):
        y = GroupBySummary() #invoke groupby class
        df1 = df  #collect dataframe from input
        dfs = []  #placeholder list for multiple data frames
        for l in avg_list:
            avg_result = l[0]  #collect the derived data column name
            df1[avg_result] = 0  #placholder value of zero, until later calculation done
            avg_field = l[1]  #collect the raw input file value
            df2 = GroupBySummary.avg_group(y, df1, avg_field, avg_result, by_field)  #invoke method for sum aggregation
            dfs.append(df2)  #append to the placeholder list
        df3 = pd.concat(dfs, axis=1, ignore_index=False, sort=True)  #convert the placeholder list into a dataframe
        print(df3.head())  #developer check monitor completness of the execution
        return df3   #return consolidated dataframe

    def avg_group(self, df, avg_field, avg_result, by_field):
        """
        List comprehension version is faster than the loop
        :param df: input dataframe, raw from the import data
        :param avg_list:  user specified fields/columns they would like to arithmetic average, mean
        :param by_field:  groupby reporting field, usually the tagging buckets made earlier in the process
        :return:  the groupby defined dataframe, apply average
        """
        cols = df.columns.tolist()  #collect list of all column fields
        cols.remove(by_field)  #remove field for calculation
        cols.remove(avg_result)  #remove field for calculation
        cols.remove(avg_field)  #remove field for calculation
        df1 = df.drop(cols, axis=1)  # get rid of all other columns you don't need, import for speed
        df1[avg_result] = (round(df1[avg_field], 0))   #copy them to the new field, round them
        df2 = df1.drop(avg_field, axis=1)  # get rid of all other columns you don't need, import for speed
        df3 = df2.groupby(by_field)  #groupby executed to aggreate data
        return df3.mean()  #apply average on the return

    def avg_group_loop(self, df, avg_list, by_field):
        """
        Loop had the same outcome as teh list comprehension but is slower
        :param df:   input dataframe, raw from the import data
        :param avg_list:  user specified fields/columns they would like to arithmetic average, mean
        :param by_field:  groupby reporting field, usually the tagging buckets made earlier in the process
        :return:   the groupby defined dataframe, apply average
        """
        cols = df.columns.tolist()  # collect all the columns in the data set
        cols.remove(by_field)  #remove field for calculation
        for list in avg_list: #for the fields in the sum_list
            df[list[0]] = round(df[list[1]].mean(), 0)  #copy them to the new field, round them
        df1 = df.drop(cols, axis=1) #get rid of all other columns you don't need, import for speed
        df2 = df1.groupby(by_field)  #groupby executed to aggreate data
        return df2.mean()   #apply sum on the return

    def stats(self, df, cols):
        df1 = df.drop(cols)
        dict = {'min': df1.min(), 'max': df1.max(), 'count': df1.count(), 'mean': df1.mean()}
        df2 = pd.DataFrame(dict)
        return df2


def main():
    x = CollectCompiledData()  #invoke class for collecting data
    y = GroupBySummary()  #invoke class for transforming and grouping data
    fname = 'report_tag_data/merge_data_tag_out_17-18.csv'  #path for collecting file
    filter_cols = None  #filter columns out of the source file, list of strings
    filter_rows = None  #filter number of rows collected from source files, integer
    a = CollectCompiledData.files_to_df(x, fname, filter_cols, filter_rows)  #collect data

    #common reporting groups:
    #TERM_BUCKET
    #VINTAGE_YEAR
    #UPB_BUCKET
    #PROPERTY STATE
    #'MONTHLY REPORTING PERIOD'
    by_field = 'PROPERTY STATE'  #typically a derived field, but could a raw field
    sum_list = [['Total_UPB', 'ORIGINAL UPB'], ['Total_Loan_Count', 'LOAN_COUNT']]  #list of lists, used to sum aggrgate data
    b = GroupBySummary.sum_iterate(y, a, sum_list, by_field)  # call pure some aggregeation group by, return df
    avg_list = [['Avg UPB', 'ORIGINAL UPB']]  #list of lists for pure mean or average of data, return dataframe
    c = GroupBySummary.avg_iterate(y, a, avg_list, by_field)  #call mean or averaged based aggreagtion
    weight = 'ORIGINAL UPB'  #field to be used for weighted average aggregation
    #list of lists for the weighted average aggregation, return data frame
    wa_list = [['Int Rate WA', 'ORIGINAL INTEREST RATE'], ['BRW FICO WA', 'BORROWER CREDIT SCORE AT ORIGINATION'],['CO BRW FICO WA', 'CO-BORROWER CREDIT SCORE AT ORIGINATION'],['LTV WA', 'ORIGINAL LOAN-TO-VALUE (LTV)'], ['CLTV WA', 'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)'], ['DTI WA', 'ORIGINAL DEBT TO INCOME RATIO']]
    e = GroupBySummary.wa_iterate_loop(y, a, wa_list, weight, by_field)  #call method for aggregation by weighted average, return data frame
    print('from main\n', e)  #developer check to monitor progress of the aggregation
    g = pd.concat([b, c, e], axis=1, ignore_index=False, sort=True)  #combine all data frames
    fnameg = ('groupby_data/state_bucket_17-18.csv')  #file name for output to be used for visualization
    j = CollectCompiledData.df_to_files(y, g, fnameg)  #export data to be used later in visualization





if __name__ == '__main__':
    main() #execution point
