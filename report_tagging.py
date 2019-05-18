import pandas as pd
import numpy as np

class GetData():
    def get_merge_loans(self, fname):

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
                         'SERVICING ACTIVITY INDICATOR', 'SOURCE_PERF', 'ORIGINATION CHANNEL',
                         'SELLER NAME', 'ORIGINAL INTEREST RATE', 'ORIGINAL UPB',
                         'ORIGINAL LOAN TERM', 'ORIGINATION DATE', 'FIRST PAYMENT DATE',
                         'ORIGINAL LOAN-TO-VALUE (LTV)',
                         'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'NUMBER OF BORROWERS',
                         'ORIGINAL DEBT TO INCOME RATIO', 'BORROWER CREDIT SCORE AT ORIGINATION',
                         'FIRST TIME HOME BUYER INDICATOR', 'LOAN PURPOSE', 'PROPERTY TYPE',
                         'NUMBER OF UNITS', 'OCCUPANCY TYPE', 'PROPERTY STATE', 'ZIP CODE SHORT',
                         'PRIMARY MORTGAGE INSURANCE PERCENT', 'PRODUCT TYPE',
                         'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'MORTGAGE INSURANCE TYPE',
                         'RELOCATION MORTGAGE INDICATOR', 'SOURCE_ACQ']

        filter_columns = ['LOAN IDENTIFIER', 'MONTHLY REPORTING PERIOD', 'SERVICER NAME',
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
                          'SERVICING ACTIVITY INDICATOR', 'SOURCE_PERF', 'ORIGINATION CHANNEL',
                          'SELLER NAME', 'ORIGINAL INTEREST RATE', 'ORIGINAL UPB',
                          'ORIGINAL LOAN TERM', 'ORIGINATION DATE', 'FIRST PAYMENT DATE',
                          'ORIGINAL LOAN-TO-VALUE (LTV)',
                          'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'NUMBER OF BORROWERS',
                          'ORIGINAL DEBT TO INCOME RATIO', 'BORROWER CREDIT SCORE AT ORIGINATION',
                          'FIRST TIME HOME BUYER INDICATOR', 'LOAN PURPOSE', 'PROPERTY TYPE',
                          'NUMBER OF UNITS', 'OCCUPANCY TYPE', 'PROPERTY STATE', 'ZIP CODE SHORT',
                          'PRIMARY MORTGAGE INSURANCE PERCENT', 'PRODUCT TYPE',
                          'CO-BORROWER CREDIT SCORE AT ORIGINATION', 'MORTGAGE INSURANCE TYPE',
                          'RELOCATION MORTGAGE INDICATOR', 'SOURCE_ACQ']

        # no filters applied, although it will probably help performance at some point
        #nrows=100
        df = pd.read_csv(fname, sep=',')

        return df

    def write_results(self, df, fname):
        df.to_csv(fname, sep=',')
        return


class DataTransform():

    def bucket_list_long(self, list_name, min, max, increment):
        # much slower
        base_list = np.arange(min, max, increment) #generate the bucket levels
        for x in base_list: #for bucket level in base list
            if x <= max: #if the bucket level is between in the min and max
                list_name += [[x, x+increment]]  #create the bounds the bucket
        return list_name  #give back the list containing bucket bounds

    def bucket_list(self, list_name, min, max, increment):  #same as bucket() but in list comprehension form
        base_list = np.arange(min, max, increment)  #generate the bucket levels
        list_name = [[x, x + increment] for x in base_list if x <= max] #generate the bucket range - WORKS
        return list_name


    def df_bucket_long(self, df, df_field, df_bucket_name, bucket_list):  #same as df_bucket_list_comp but in for loop form
        #much slower
        for i in range(len(df[df_field])):  #for value in df series
            for b in bucket_list:  #for the bucket min, max in the bucket list provided
                if b[0] <= df.loc[i, df_field] and df.loc[i, df_field] < b[1]:  #if the value in the df series is between the bucket list min, max
                    df.loc[i, df_bucket_name] = b[0]  #update the df bucket name value to be the value of the bucket max
        return df  #return the dataframe after update


    def df_bucket(self, df, df_field, df_derived, bucket_list):
        # data type of df series and buckets should match
        df[df_derived] = None
        df[df_derived] = [b[0] for i in range(len(df[df_field])) for b in bucket_list if b[0] <= df.loc[i, df_field] and df.loc[i, df_field] < b[1]]
        return df #return the dataframe after update


    def nan_fix(self, df, df_field, df_derived, enrich):
        df[df_field].to_csv(('report_tag_data/pre-' + df_derived + '.csv'), sep=',') #write out for validation
        df[df_field].fillna(value=enrich, inplace=True)  #edit the data set and overwrite
        df[df_field].to_csv(('report_tag_data/post-'+df_derived+'.csv'), sep=',')  #write otu for validation or to use for intermediate visualization
        return df  # return the dataframe after update


    def count_long(self, df, x, y): #for loop style
        for i in df[x]:  #create a loan count number, to be groupedby later
            if i != 'NaN' or i != 0:
                df[y] = 1
            else:
                df[y] = 0
        return df


    def count(self, df, x, y): #list compression style
        df[y] = [1 for i in df[x] if i != 'NaN' or i != 0] #create a loan count number, to be groupedby later
        return df


    def vintage_yr(self, df, x, y):
        df[y] = pd.DatetimeIndex(df[x]).year #convert the data to a data format, apply year method.
        return df


    def vintage_mnth(self, df, x, y):
        df[y] = pd.DatetimeIndex(df[x]).month  #convert the data to a data format, apply month method.
        return df


class CallTransform():

    def derive_loan_count(self, df, df_field, df_derived):
        loan_num = df_field
        loan_count = df_derived
        #acq_df_ln_ct = count_long(df, loan_num, loan_count) #long one, skip
        x = DataTransform()
        acq_df_ln_ct = DataTransform.count(x, df, loan_num, loan_count)
        print(acq_df_ln_ct.head())
        return acq_df_ln_ct


    def derive_vintage(self, df, df_field, df_derived_1, df_derived_2):
        x = DataTransform()
        df_y = DataTransform.vintage_yr(x, df, df_field, df_derived_1)
        print(df_y.head())
        df_m = DataTransform.vintage_mnth(x, df_y, df_field, df_derived_2)
        print(df_m.head())
        return df_m


    def derive_buckets(self, df, df_field, df_derived, bucket_min, bucket_max, bucket_incr, nan_enrich):
        #create buckets
        bucket_list = []
        x = DataTransform()
        df_fix = DataTransform.nan_fix(x, df, df_field, df_derived, nan_enrich)
        #print(df_fix.head())  #developer check
        bucket = DataTransform.bucket_list(x, bucket_list, bucket_min, bucket_max, bucket_incr)
        #print(bucket) #developer check
        #apply buckets
        df_buck = DataTransform.df_bucket(x, df_fix, df_field, df_derived, bucket)
        #df_buck = DataTransform.df_bucket_long(x, df_fix, df_field, df_derived, bucket) #long one
        print(df_buck.head())  #developer test
        return df_buck


def main():
    #invoke collect data class
    y = GetData()
    #invoke data transformation classes
    z = CallTransform()

    # collect merged dat set
    in_file = 'merge_data/perf_acq_data_17-18.csv'
    a = GetData.get_merge_loans(y, in_file)
    # print(a.shape) #developer check


    #create loan count field
    loan_num = 'LOAN IDENTIFIER'  #known raw data field
    loan_count = 'LOAN_COUNT'  #derived data field for future grouping
    b = CallTransform.derive_loan_count(z, a, loan_num, loan_count)

    #creat vintage bucket
    orig_dt = 'ORIGINATION DATE'  #known raw data field
    vintage_y = 'VINTAGE_YEAR'  #derived data field for future grouping
    vintage_m = 'VINTAGE_MONTH'  #derived data field for future grouping
    c = CallTransform.derive_vintage(z,b, orig_dt, vintage_y, vintage_m)

    #create buckets WORKS
    #LTV bucket
    ltv_min = 0  #numeric parameter to create reporting bands/buckets
    ltv_max = 200  #numeric parameter to create reporting bands/buckets
    ltv_incr = 10  #numeric parameter to create reporting bands/buckets
    ltv_enrich = 80  #numeric parameter used to override nan values
    s_LTV = 'ORIGINAL LOAN-TO-VALUE (LTV)'  #known raw data field
    b_LTV = 'LTV_BUCKET'  #derived data field for future grouping
    d = CallTransform.derive_buckets(z, c, s_LTV, b_LTV, ltv_min, ltv_max, ltv_incr, ltv_enrich)

    #UPB bucket
    upb_min = 0  #numeric parameter to create reporting bands/buckets
    upb_max = 2000000  #numeric parameter to create reporting bands/buckets
    upb_incr = 50000  #numeric parameter to create reporting bands/buckets
    upb_enrich = 200000  #numeric parameter used to override nan values
    s_UPB = 'ORIGINAL UPB'  #known raw data field
    b_UPB = 'UPB_BUCKET'  #derived data field for future grouping
    e = CallTransform.derive_buckets(z, d, s_UPB, b_UPB, upb_min, upb_max, upb_incr, upb_enrich)

    #FICO bucket
    fico_min = 300.0  #numeric parameter to create reporting bands/buckets
    fico_max = 900.0  #numeric parameter to create reporting bands/buckets
    fico_incr = 50.0  #numeric parameter to create reporting bands/buckets
    fico_enrich = 750.0  #numeric parameter used to override nan values
    s_FICO = 'BORROWER CREDIT SCORE AT ORIGINATION'  #known raw data field
    b_FICO = 'FICO_BUCKET'  #derived data field for future grouping
    f = CallTransform.derive_buckets(z, e, s_FICO, b_FICO, fico_min, fico_max, fico_incr, fico_enrich)

    #TERM bucket
    term_min = 0  #numeric parameter to create reporting bands/buckets
    term_max = 480  #numeric parameter to create reporting bands/buckets
    term_incr = 60  #numeric parameter to create reporting bands/buckets
    term_enrich = 360  #numeric parameter used to override nan values
    s_term = 'ORIGINAL LOAN TERM'  #known raw data field
    b_term = 'TERM_BUCKET'  #derived data field for future grouping
    g = CallTransform.derive_buckets(z, f, s_term, b_term, term_min, term_max, term_incr, term_enrich)

    #Interest Rate bucket
    int_rt_min = 0  #numeric parameter to create reporting bands/buckets
    int_rt_max = 15  #numeric parameter to create reporting bands/buckets
    int_rt_incr = 0.125  #numeric parameter to create reporting bands/buckets
    int_rt_enrich = 4.25  #numeric parameter used to override nan values
    s_int_rt = 'ORIGINAL INTEREST RATE'  #known raw data field
    b_int_rt = 'INT_RT_BUCKET'  #derived data field for future grouping
    h = CallTransform.derive_buckets(z, g, s_int_rt, b_int_rt, int_rt_min, int_rt_max, int_rt_incr, int_rt_enrich)


    #write output file
    out_file = 'report_tag_data/merge_data_tag_out_17-18.csv'
    i = GetData.write_results(y, h, out_file)



if __name__ == '__main__':
    main()


