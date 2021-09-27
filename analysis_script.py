import pandas as pd
import numpy as np
import os
from functools import reduce
from sys import argv

def read_file(filename):
    """
    Read file, check whether it contains proper
    columns (based on first one) and return 
    DataFrame.
    """
    #read file, skiping rows with metadata
    file = pd.read_csv(filename,"\t",skiprows=2)
    #check columns (whether first one is subject)
    if np.any(file.columns.str.contains('Subject')):
        #if columns are proper, return file
        return file
    else:
        #if columns are not proper, read with skipping
        #different n of rows
        file = pd.read_csv(filename,"\t",skiprows=3)
        #drop unnecessary rows
        file = file.dropna(subset = ['Subject'])
        #return file
        return file

def time_correction(data):
    """
    Corrects time in data from Presentation. 
    Reduce all Time column by time value of first event, 
    then divides by 10000 (to turn into ms).
    Returns whole DataFrame.
    """
    data['Time'] = (data['Time'] - data['Time'].iloc[0])/10000
    return data

def get_arg_block(data):
    """Get arg of blocks in data"""
    #get condition (all rows with word block in Code col)
    cond = ['block' in n.lower() for n in data['Code']]
    return (data
            .loc[cond]
            .index)

def get_list_endpoints(arg_data_part):
    """ 
    Get arg of starting and ending block, returns 
    list with shape [[arg_start,arg_end]...]
    """
    return [[arg_data_part[arg],arg_data_part[arg+1]] for \
            arg in range(0,len(arg_data_part),2)]

def present_target(data,start_time,end_time):
    """
    Check for the presence of the target.
    """
    return np.any((data
                   .loc[start_time:end_time,'Code']
                   .str.contains('target')))

def get_time_point_results(data,time_points):
    """
    Get results from data, for a given time_point.
    Return a dictionary with colnames linked to values.
    """
    #get code event
    code_name = data.loc[time_points[0],'Code']
    #get starting point of event
    start_time = data.loc[time_points[0],'Time']
    #calculate enging point of event
    end_time = (data.loc[time_points[1],'Time']-start_time)
    #check the presence of a target in event
    target_val = present_target(data,
                                time_points[0],
                                time_points[1])
    #return a tuple with all
    return {'Code':code_name,
            'StartTime':start_time,
            'EndTime':end_time,
            'target':target_val}

def merge_blocks(previous_block,endpoints):
    """
    Merge previous block with new one.
    """
    tmp_series = pd.Series(endpoints)
    try:
        return previous_block.append(tmp_series,
                                     ignore_index=True)
    except:
        return pd.DataFrame().append(tmp_series,
                                     ignore_index=True)

def get_block_result(data):
    """
    Pipeline for obtaining block results.
    """
    #Get arguments of blocks
    blocks_arg = get_arg_block(data)
    #Get endpoints for each block
    endpoints = get_list_endpoints(blocks_arg)
    #Fix for first iteration in map
    endpoints.insert(0,[0,0])
    #Get iterator for each time block
    time_block_result = map(lambda x : get_time_point_results(data,x),
                            endpoints)
    #Get result
    return reduce(merge_blocks,time_block_result)

def get_cue_results(data,placeholder=pd.DataFrame()):
    """
    Get results for cue, without target presence column.
    """
    #get args of cue occurences
    args = (data
            [data['Code'].str.contains('cue')]
            .index)
    #get Series with event codes
    placeholder['Code'] = data.loc[args,'Code']
    #calculate ending times
    placeholder['EndTime'] = (data.loc[args+1]['Time'].values -\
                              data.loc[args]['Time'])
    #get start times Series
    placeholder['StartTime'] = data.loc[args,'Time']
    #return placeholder with all variables
    return placeholder

def get_subject_code(data):
    return data['Subject'].iloc[0]

def get_run(filename):
    return filename.split('_')[-1][:4]

def save_result(result,name,run,path):
    result.to_csv(f'{path}results_{name}_{run}.csv')
        
def single_beh_results(data,name,run):
    res = pd.value_counts(data['Stim Type'])
    dict_run = [(name,run,n) for n in res.index]
    index = pd.MultiIndex.from_tuples(dict_run,names=['Subject','Run','Lvl'])
    return pd.DataFrame(res.values,index=index,columns=['Results'])


def save_behaviour(name,new_series,path):
    try:
        file = pd.read_excel(f"beh_results_{name}.xlsx",header=[0],index_col=[0,1,2])
        file = file.append(new_series)
        file.to_excel(f"beh_results_{name}.xlsx")
    except:
        new_series.to_excel(f"{path}beh_results_{name}.xlsx")
        
def run_script(path):
    file_name_list = os.listdir(path)
    for file_name in file_name_list:
        try:
            #Read file
            data = read_file(f"{path}{file_name}")
            #get subject code
            name = get_subject_code(data)
            #get code of run
            run = get_run(file_name)
            #Perform time correction
            data = time_correction(data)
            #get block results
            result = get_block_result(data)
            #Append cue results, fill Nan with 0
            #and reset index, for aesthethical purposes
            result = (result
                      .append(get_cue_results(data))
                      .fillna(0)
                      .reset_index(drop=True))
            save_result(result,name,run,path)
            behaviour = single_beh_results(data,name,run)
            save_behaviour(name,behaviour,path)
        except Exception as e:
            print(f"Problem with {file_name}, problem: {e}")
        
if __name__ == '__main__':
    if len(argv) > 1:
        run_script(argv[1])
    else:
        path = input('Pass path to folder:')
        run_script(path)