
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 28 11:58:42 2024
@author: duvvuri.b
"""
import xarray as xr
import geopandas as gpd
import pandas as pd
# import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm
import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri
from rpy2.robjects.packages import importr
import time
from glob import glob
import os
import sys

import spatial_variable as sp
import river_network_data as rnd

# Activate automatic conversion between R and Python objects
pandas2ri.activate()

# Import required R packages
base = importr('base')
utils = importr('utils')
data_table = importr('data.table')
foreign = importr('foreign')
dplyr = importr('dplyr')

start_time = time.perf_counter()
temp_time = start_time

sp = sp.spatial_variable()
rnd = rnd.network_info()

def process_chunk(chunk_data, lats, lons):
    sel_data = chunk_data.sel(lat=lats, lon=lons, method='nearest')
    diagonal_data = np.diagonal(sel_data.data, axis1=1, axis2=2)
    return np.round(diagonal_data, decimals=6)

for file_counter, file_num in enumerate(rnd.all_pfs):
    #if file_num!= 54:
    #    continue
    data_WID = rnd.get_WID(file_num)
    cat_data = rnd.get_cat(file_num)
    data2,data_WID =  rnd.sanity_checknetwork(cat_data, data_WID) 
   
    # Loop over time and retreive data at coordinates
    latitude = data2['geometry'].centroid.y 
    longitude = data2['geometry'].centroid.x 
    print("Reading file_num {}".format(file_num))

    ############################################# step 3 
    #sp_filesread = sp.get_spfiles_list()
    #num_rows = data_WID.shape[0]
    #df_step3 = np.zeros((num_rows,len(sp_filesread)+1))
    #col_names = ['RIVID']
    #t  = 0
    #for enum, f_address in enumerate(sp_filesread):

    #    data_precep = xr.open_dataset(f_address)
    
    #    ##%read variable lwe_thickness data
    #    variables = list(data_precep.data_vars.keys())

    #    var_time = f_address.split('.')[-2]
    #    #print(var_time)
    #    if var_time >= "20250101":
    #        break

    #    col_names.append(var_time)
    #    variable = sp.variable 
        
    #    sel_t = np.diagonal(data_precep[variable].sel(lat=latitude.tolist(), lon=longitude.tolist(),method='nearest').data)
        
    #    if enum == 0:
    #        df_step3[:, 0] = data_WID['RIVID']
    #        df_step3[:, enum+1] = sel_t.flatten()

    #    df_step3[:, enum+1] = sel_t.flatten()
        
    ##

    ############################################# step 3
    sp_filesread = sp.get_spfiles_list()
    num_rows = data_WID.shape[0]

    print("Processing spatial files...")
    chunk_size = 10  # Process 10 time steps at once, adjust as needed
    all_data = []
    time_values = []

    for i in tqdm(range(0, len(sp_filesread), chunk_size)):
        chunk_files = sp_filesread[i:i+chunk_size]
        chunk_data_list = []
        
        for f_address in chunk_files:
            ds = xr.open_dataset(f_address)
            chunk_data_list.append(ds[sp.variable])
            var_time = f_address.split('.')[-2]
            time_values.append(var_time)
            if var_time >= "20250101":
                break
        
        if not chunk_data_list:
            break

        chunk_data = xr.concat(chunk_data_list, dim='time')
        chunk_result = process_chunk(chunk_data, latitude.tolist(), longitude.tolist())
        all_data.append(chunk_result)

    # Combine all processed data
    final_data = np.concatenate(all_data, axis=0)

    # Create DataFrame
    df_step3 = pd.DataFrame(final_data, columns=time_values)
    df_step3.insert(0, 'RIVID', data_WID['RIVID'])

    df_step3.to_csv(f'E:/precep_processed_1105/step3_{file_num}.csv', index=False)
    print(f"Step 3 completed for file {file_num}")

    del results, data_precep_concat
    time_after_first = time.perf_counter()    
    print(f"time taken after step 3: {time_after_first - start_time:.6f} seconds")

    ################################################## Step 4 (R code integration)
    # Run the R code
    robjects.r(rnd.get_step4_code())

    # Get a reference to the R function
    r_calculate_upstream_average = robjects.r['calculate_upstream_average']

    # Convert Python dataframes to R dataframes
    r_data_WID = robjects.conversion.py2rpy(data_WID)

    # Split the dataframe into chunks of 50 time columns plus RIVID
    chunk_size = 50
    time_columns = df_step3.columns[1:]  # Exclude RIVID column
    chunks = [time_columns[i:i+chunk_size] for i in range(0, len(time_columns), chunk_size)]

    df_step4_chunks = []

    for chunk_idx, chunk_columns in tqdm(enumerate(chunks)):
        # Create a subset of df_step3 with RIVID and the current chunk of time columns
        df_step3_chunk = df_step3[['RIVID'] + chunk_columns.tolist()]
    
        # Convert the chunk to R dataframe
        r_df_step3_chunk = robjects.conversion.py2rpy(df_step3_chunk)
    
        # Define varnames_col for the chunk
        r_varnames_col = robjects.StrVector(df_step3_chunk.columns)
    
        # Call the R function for the chunk
        result_r = r_calculate_upstream_average(r_data_WID, r_df_step3_chunk, file_num, r_varnames_col)
        
        # Convert the result back to a pandas dataframe
        df_step4_chunk = robjects.conversion.rpy2py(result_r)

        df_step4_chunks.append(df_step4_chunk)

    # Concatenate all chunks
    df_step4 = pd.concat(df_step4_chunks, axis=1)

    # Remove duplicate RIVID columns (keep only the first one)
    df_step4 = df_step4.loc[:, ~df_step4.columns.duplicated()]

    df_step4 = pd.DataFrame(df_step3, columns=col_names)
    df_step4.to_csv(f'E:/precep_processed_1105/step4_{file_num}.csv',index= False)


    time_after_first = time.perf_counter()
    print(f"Time taken for {file_num} : {time_after_first - temp_time:.6f} seconds")
    temp_time = time_after_first

    # ... (rest of the code remains the same)

