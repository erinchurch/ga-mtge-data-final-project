import pandas as pd

"""
Once the consolidate, multi-period files have been created. 
This code is expected to merge those data files. 
For the FNMA loan level performance data, the Acqusition and Performance data is merged on Loan Number.
In the original specification of the source files, the loan number is shared key between them. 

"""

class GetMergeData():

    def get_acq_loans(self, fname):

        df = pd.read_csv(fname, sep=",")
        return df

    def get_perf_loans(self,fname):

        df = pd.read_csv(fname, sep=',')
        return df

    def merge(self, df1, df2):
        df_new = pd.merge(df1, df2, on='LOAN IDENTIFIER', validate='m:1' )
        return df_new

def main():
    y = GetMergeData()  #invoke class for fetching the data and merging it
    acq_in_file = 'draft_multi_file_data/acq_file_02-03.csv'  #identify the file and directory of the consolidated data sets
    a = GetMergeData.get_acq_loans(y, acq_in_file)  #collect acqusition data into a data frame
    #print(a.shape) #developer check
    perf_in_file = 'draft_multi_file_data/perf_file_02-03.csv' #idenify file and directory fo consolidated data
    b = GetMergeData.get_perf_loans(y, perf_in_file)  #collect performance data into a dataframe
    #print(b.shape)  #developer check
    c = GetMergeData.merge(y, b, a)  #merge the performance and acqusition data frames
    # print(c.shape) #developer test
    c.to_csv('draft_merge_data/perf_acq_data_02-03.csv')  #write merged data to a new file


if __name__ =='__main__':
    main()  #execution point
