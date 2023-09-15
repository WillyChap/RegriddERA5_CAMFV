import xarray as xr
import glob
import os
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
import dask
import dask.threaded
import dask.multiprocessing
from dask import delayed
import time
from netCDF4 import Dataset

#### settings
start_date = '1979-01-06'
end_date = '1979-01-09' #make sure this date is after the start date... 
interval_hours = 1 #what hour interval would you like to get? 
batch_size = 2 #it's dangerous to do alot you might run out of memory... this processes X days per batch...
FPout = '/glade/scratch/wchapman/ERA5_regrid_out/' #where do you want the files stored?
prefix_out = 'ERA5_e5.oper.ml.v3' #what prefix do you want the files stored with?
use_multithreading = False  # Set to True for multi-threading or False for multi-processing
#### settings

def find_strings_with_substring(string_list, substring):
    # Initialize an empty list to store matching strings
    matching_strings = []

    # Iterate through the list
    for string in string_list:
        # Check if the specified substring is present in the current string
        if substring in string:
            matching_strings.append(string)

    # Return the list of matching strings
    return matching_strings

def flatten_list(input_list):
    flattened_list = []
    for item in input_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list

##function get file paths ... 
def fp_dates_wanted(Dateswanted):
    years_wanted = Dateswanted[:].year
    months_wanted = Dateswanted[:].month
    day_wanted = Dateswanted[:].day
    
    list_yrm =[]
    for ywmw in zip(years_wanted,months_wanted):
        list_yrm.append(str(ywmw[0])+f'{ywmw[1]:02}')
    
    fp_t = []
    fp_u = []
    fp_v = []
    fp_q = []
    fp_ps = []
    
    lastday = str(Dateswanted[-1])[:10]
    
    for yrm_fp in np.unique(list_yrm):
        for dayday in np.unique(day_wanted):
            
            
            fp_u.append(sorted(glob.glob('/glade/collections/rda/data/ds633.6/e5.oper.an.ml/'+yrm_fp+'/'+'*_u*'+yrm_fp+f'{dayday:02}'+'*.nc')))
            fp_v.append(sorted(glob.glob('/glade/collections/rda/data/ds633.6/e5.oper.an.ml/'+yrm_fp+'/'+'*_v*'+yrm_fp+f'{dayday:02}'+'*.nc')))
            fp_t.append(sorted(glob.glob('/glade/collections/rda/data/ds633.6/e5.oper.an.ml/'+yrm_fp+'/'+'*_t*'+yrm_fp+f'{dayday:02}'+'*.nc')))
            fp_q.append(sorted(glob.glob('/glade/collections/rda/data/ds633.6/e5.oper.an.ml/'+yrm_fp+'/'+'*_q*'+yrm_fp+f'{dayday:02}'+'*.nc')))
            fp_ps.append(sorted(glob.glob('/glade/collections/rda/data/ds633.6/e5.oper.an.ml/'+yrm_fp+'/'+'*_sp*'+yrm_fp+f'{dayday:02}'+'*.nc')))
            
            if yrm_fp[:4]+'-'+yrm_fp[4:]+'-'+f'{dayday:02}' == lastday:
                break

    fp_u = flatten_list(fp_u)
    fp_v = flatten_list(fp_v)
    fp_t = flatten_list(fp_t)
    fp_q = flatten_list(fp_q)
    fp_ps = flatten_list(fp_ps)
    
    files_dict ={'u':np.unique(fp_u),'v':np.unique(fp_v),'t':np.unique(fp_t),'q':np.unique(fp_q),'ps':np.unique(fp_ps)}
    
    
    return files_dict 

from dask.diagnostics import ProgressBar

def make_nc_files_optimized(files_dict, Dateswanted, Dayswanted):
    """
    Optimized function to perform a combination of ERA5 prognostic variables
    using Dask with specified resources.

    Parameters:
    - files_dict: A dictionary of files for each variable.
    - Dateswanted: List of dates this is days with hours.
    - Dayswanted: List of days this is the days sans hours (a little redundant).

    Returns:
    - delayed_writes: List of delayed write operations.
    """
    # Set up Dask for parallelism
    dask.config.set(scheduler='processes')  # Use processes for parallelism
    Static_zheight = xr.open_dataset('/glade/u/home/wchapman/RegriddERA5_CAMFV/static_operation_ERA5_zhght.nc')
    for dw in Dayswanted:
        print(str(dw)[:10])
        substring_match = str(dw)[:4] + str(dw)[5:7] + str(dw)[8:10]
        smatch_u = find_strings_with_substring(files_dict['u'], substring_match)
        smatch_v = find_strings_with_substring(files_dict['v'], substring_match)
        smatch_t = find_strings_with_substring(files_dict['t'], substring_match)
        smatch_q = find_strings_with_substring(files_dict['q'], substring_match)
        smatch_ps = find_strings_with_substring(files_dict['ps'], substring_match)
        
        DS_u = xr.open_mfdataset(smatch_u, parallel=True)
        sel_times = Dateswanted.intersection(DS_u['time'])
        DS_v = xr.open_mfdataset(smatch_v, parallel=True).sel(time=sel_times)
        DS_t = xr.open_mfdataset(smatch_t, parallel=True).sel(time=sel_times)
        DS_q = xr.open_mfdataset(smatch_q, parallel=True).sel(time=sel_times)
        DS_ps = xr.open_mfdataset(smatch_ps, parallel=True).sel(time=sel_times)
        
        print('loading')
        DS = xr.merge([DS_u.sel(time=sel_times), DS_v, DS_t, DS_q])
        print('loaded')
        delayed_writes = []
        outfile_compile = []
        for ee, tt in enumerate(DS['time']):
            hourdo = DS['time.hour'][ee]
            
            #delayed write UVTQ
            datstr = str(dw)[:4] + str(dw)[5:7] + str(dw)[8:10] + f'{hourdo:02}'
            out_file = FPout + '/' + prefix_out + '.uvtq.' + datstr + '.nc'
            delayed_write = delayed(DS.sel(time=tt).squeeze().to_netcdf)(out_file)
            delayed_writes.append(delayed_write)
            
            #delayed write PS
            out_file = FPout + '/' + prefix_out + '.ps.' + datstr + '.nc'
            DS_ps['Z_GDS4_SFC'] = xr.zeros_like(DS_ps['SP'])
            DS_ps['Z_GDS4_SFC'][:, :] = Static_zheight['Z_GDS4_SFC'].values
            delayed_write = delayed(DS_ps.sel(time=tt).squeeze().to_netcdf)(out_file)

    # Compute the delayed write operations concurrently
        with ProgressBar():
            delayed_writes = dask.compute(*delayed_writes)

    return delayed_writes

def add_staggered_grid_delayed(FPout, prefix_out):
    dask.config.set(scheduler='processes')  # Use processes for parallelism
    """
    Perform staggered grid modification on multiple NetCDF files with Dask and delayed writes.

    This function opens a list of NetCDF files, adds variables to them in a staggered grid format, and saves the modified
    datasets with delayed write operations to avoid excessive memory usage. It also removes the original files.

    Parameters:
    - FPout (str): The path to the directory containing the NetCDF files.
    - prefix_out (str): The prefix used in the filenames to identify the files to be processed.

    Returns:
    - all_files (list): A list of all processed file paths.
    """

    # List all files with the specified prefix
    all_files = sorted(glob.glob(FPout + '/' + prefix_out + '.uvtq.??????????.nc'))
    print('bingo: ',FPout + '/' + prefix_out + '.uvtq.??????????.nc')
    # List to store delayed write operations
    delayed_writes = []
    
    for fdfd in all_files:
        print(fdfd)
        # Open the file with a delayed operation
        delayed_open = delayed(xr.open_dataset)(fdfd)
        
        # Add variables with delayed operations
        def add_variables(BB):
            bbus = xr.zeros_like(BB['U']).to_dataset(name='US')
            bbus['US'][:, :] = BB['U']
            bbvs = xr.zeros_like(BB['V']).to_dataset(name='VS')
            bbvs['VS'][:, :] = BB['V']
            bball = xr.merge([BB, bbus, bbvs]).chunk()
            return bball
        
        delayed_add_vars = delayed(add_variables)(delayed_open)
        
        # Define the output filename with staggered grid
        staggered_filename = fdfd[:-13] + 's.' + fdfd[-13:]
        
        # Save the modified dataset with delayed operation
        delayed_write = delayed(delayed_add_vars.to_netcdf)(staggered_filename)
        delayed_writes.append(delayed_write)
        
        # Remove the original file
        # os.remove(fdfd)
    
    # Define a chunk size for computing delayed writes
    chunk_size = 40  # Adjust as needed
    
    # Compute delayed writes in chunks
    for i in range(0, len(delayed_writes), chunk_size):
        chunk = delayed_writes[i:i + chunk_size]
        with ProgressBar():
            dask.compute(*chunk)
    
    return all_files


if __name__ == '__main__':
    print('timing!')
    start_time = time.time()  # Record the start time
    print("search for the right days")
    Dateswanted = pd.date_range(start=start_date,end=end_date,freq=str(interval_hours)+'H')
    Dayswanted = pd.date_range(start=start_date,end=end_date,freq=str(interval_hours)+'D')
    Static_zheight = xr.open_dataset('/glade/u/home/wchapman/RegriddERA5_CAMFV/static_operation_ERA5_zhght.nc')
    files_dict=fp_dates_wanted(Dateswanted)
    print('starting processing')
    delayed_writes = make_nc_files_optimized(files_dict, Dateswanted, Dayswanted)
    #print(delayed_writes)
    print('adding staggered')
    allf, bb = add_staggered_grid_delayed(FPout,prefix_out)
    elapsed_time = time.time() - start_time
    print(f" executed in {elapsed_time} seconds")
