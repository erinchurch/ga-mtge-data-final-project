import pandas as pd
import glob


"""
Mortgage data can come in multiple files from the source.
The objective of this code is to iterate through a directory of like files.
The the like data files are consolidated into a single dataframe.
Then the data is written to a single file. 
"""
"""
TODO:
If running on AWS or a larger machine, remove filters, currently set to 100k records.
If modifying source files or repurposing, change method names, all the file names, and the header files of each data import
"""


class GetDataMultiFile():

    def files_acq_to_df(self, fname, nrow):
        #designed for FNMA Acquisition file data, method can be further generalized to support any type of file

        #raw files do not come with a header, header specified in a separate document
        df_columns = pd.read_csv("../FNMA-raw-data/some-acquisition-dev/header_aquisition_file.txt", sep=",")
        columns = df_columns['Field Name']
        headers = columns.tolist()

        #Acquisition file data handling
        filter_columns = ['LOAN IDENTIFIER', 'ORIGINAL INTEREST RATE', 'ORIGINAL UPB',
                          'ORIGINATION DATE', 'ORIGINAL LOAN-TO-VALUE (LTV)',
                          'ORIGINAL COMBINED LOAN-TO-VALUE (CLTV)', 'ORIGINAL DEBT TO INCOME RATIO',
                          'BORROWER CREDIT SCORE AT ORIGINATION', 'CO-BORROWER CREDIT SCORE AT ORIGINATION']
        # Rows filtered to reduce execution time for demo, nrows=100k records
        #usecols=filter_columns #available for reduced analysis
        # index = 'LOAN IDENTIFIER'  #not necessary to make the loan number the index, but if using an index, use loan number.
        df = pd.read_csv(fname, sep="|", header=None, names=headers,nrows=nrow)
        return df


    def files_perf_to_df(self,fname, nrow):
        # designed for FNMA Performance file data, method can be further generalized to support any type of file

        # raw files do not come with a header, header specified in a separate document
        df_columns = pd.read_csv('../FNMA-raw-data/some-performance-dev/performance_file_headers.txt', sep=',')
        columns = df_columns['Field Name']
        headers = columns.tolist()

        # Performance file data handling
        # Rows filtered to reduce execution time for demo, nrows=100k records
        # usecols=filter_columns #available for reduced analysis
        # index = 'LOAN IDENTIFIER'  #not necessary to make the loan number the index, but if using an index, use loan number.
        df = pd.read_csv(fname, sep='|', header=None, names=headers,nrows=nrow)
        return df

    def df_to_files(self, df_name, fname):
        #write consolidated dataframe back to a file
        df_name.to_csv(fname, sep=',')
        return


class ProcessFiles():
    def acq_files(self, nrow):
        x = GetDataMultiFile()  #invoke the get data class
        files_acq = glob.glob('../FNMA-raw-data/some-acquisition/*')  #identify the directory that will contain like files
        dfs = []  #placeholder list for the intermediate data
        for file in files_acq:    #iterate through file int the directory
            # tag the data with its original source, intended to assist later debugging and transparency in final data
            name = file.replace('../FNMA-raw-data/some-acquisition/', '')
            df = GetDataMultiFile.files_acq_to_df(x, file, nrow) #pull individual file data into dataframe
            df['SOURCE'] = name #append file source name to data set
            print(df.head())  #allows one to monitor in consol the progress of execution
            dfs.append(df)   #add individual file data to the placeholder list
        df_all = pd.concat(dfs, axis=0, ignore_index=True)  #convert the placeholder list to a dataframe
        name_all = 'multi_file_data/acq_file_17-18.csv'  #name file and delivery directory
        GetDataMultiFile.df_to_files(x, df_all, name_all)  #write the consolidate dataframe out as a file
        return df_all

    def perf_files(self, nrow):
        x = GetDataMultiFile()  #invoke the get data class
        files_perf = glob.glob('../FNMA-raw-data/some-performance/*')  #identify the directory that will contain like files
        dfs = []  #placeholder list for the intermediate data
        for file in files_perf:  #iterate through file int the directory
            #tag the data with its original source, intended to assist later debugging and transparency in final data
            name = file.replace('../FNMA-raw-data/some-performance/', '')
            df = GetDataMultiFile.files_perf_to_df(x, file, nrow)  #pull individual file data into dataframe
            df['SOURCE'] = name  #append file source name to data set
            print(df.head()) #allows one to monitor in consol the progress of execution
            dfs.append(df)  #add individual file data to the placeholder list
        df_all = pd.concat(dfs, axis=0, ignore_index=True)  #convert the placeholder list to a dataframe
        name_all = 'multi_file_data/perf_file_17-18.csv'  #name file and delivery directory
        GetDataMultiFile.df_to_files(x, df_all, name_all)  #write the consolidate dataframe out as a file
        return df_all


def main():

    w = ProcessFiles() #invoke processing class
    nrow = 10000
    df_all_acq = ProcessFiles.acq_files(w, nrow)  #invoke acqusition file process
    # print(df_all_acq.shape)  #developer check
    df_all_perf = ProcessFiles.perf_files(w, nrow)  #invoke performance file process
    # print(df_all_perf.shape)  #developer check


if __name__=='__main__':
    main()  #execution point

