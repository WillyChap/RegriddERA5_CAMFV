import xarray as xr
import glob
import os
import numpy as np
import pandas as pd
from dask.diagnostics import ProgressBar
import time
from dask import delayed
from dask import delayed, persist
import dask

#### settings !!! MODIFY THIS BLOCK
start_date = '1979-01-06'
end_date = '1979-01-09' #make sure this date is after the start date... 
interval_hours = 1 #what hour interval would you like to get? [i.e: 1 = 24 files/day, 6 = 4 files/day]
FPout = '/glade/scratch/wchapman/ERA5_regrid_out/' #where do you want the files stored?
prefix_out = 'ERA5_e5.oper.ml.v3' #what prefix do you want the files stored with?
#### settings !!! MODIFY THIS BLOCK

if 'client' in locals():
    client.shutdown()
    print('...shutdown client...')
else:
    print('client does not exist yet')
    
###dask NCAR client: 
from distributed import Client
from ncar_jobqueue import NCARCluster

cluster = NCARCluster(project='P54048000',walltime='11:00:00')
cluster.scale(40)
client = Client(cluster)
client
###dask NCAR client: 

#assert that dates wanted > 0

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

def make_nc_files(files_dict,Dateswanted,Dayswanted):    
    for dw in Dayswanted:
        print(str(dw)[:10])
        substring_match = str(dw)[:4]+str(dw)[5:7]+str(dw)[8:10]
        smatch_u = find_strings_with_substring(files_dict['u'], substring_match)
        smatch_v = find_strings_with_substring(files_dict['v'], substring_match)
        smatch_t = find_strings_with_substring(files_dict['t'], substring_match)
        smatch_q = find_strings_with_substring(files_dict['q'], substring_match)
        smatch_ps = find_strings_with_substring(files_dict['ps'], substring_match)
        DS_u= xr.open_mfdataset(smatch_u)
        sel_times = Dateswanted.intersection(DS_u['time'])
        DS_v= xr.open_mfdataset(smatch_v).sel(time=sel_times)
        DS_t= xr.open_mfdataset(smatch_t).sel(time=sel_times)
        DS_q= xr.open_mfdataset(smatch_q).sel(time=sel_times)
        DS_ps= xr.open_mfdataset(smatch_ps).sel(time=sel_times)
        print('loading')
        DS=xr.merge([DS_u.sel(time=sel_times),DS_v,DS_t,DS_q]).load()
        print('loaded')
        
        for ee,tt in enumerate(DS['time']):
            hourdo = DS['time.hour'][ee]
            
            datstr = str(dw)[:4]+str(dw)[5:7]+str(dw)[8:10]+f'{hourdo:02}'
            #DS.sel(time=tt).squeeze().to_netcdf()
            out_file=+'/' +prefix_out +'.uvtq.'+ datstr+'.nc'
            write_job = DS.sel(time=tt).squeeze().to_netcdf(out_file,compute=False)
            with ProgressBar():
                print(f"Writing to {out_file}")
                write_job.compute()      
            print(out_file) 
            out_file=FPout+'/' +prefix_out +'.ps.'+ datstr+'.nc'
            DS_ps['Z_GDS4_SFC'] = xr.zeros_like(DS_ps['SP'])
            DS_ps['Z_GDS4_SFC'][:,:]=Static_zheight['Z_GDS4_SFC'].values
            write_job = DS_ps.sel(time=tt).squeeze().to_netcdf(out_file,compute=False)
            with ProgressBar():
                print(f"Writing to {out_file}")
                write_job.compute()    
            print(out_file) 

    return DS,DS_ps


def add_staggered_grid(FPout,prefix_out):
    
    prefix_out = 'test_out_'
    all_files = sorted(glob.glob(FPout+'/'+prefix_out+'??????????.nc'))
    
    for fdfd in all_files:
        print(fdfd)
        BB = xr.open_dataset(fdfd)
        bbus = xr.zeros_like(BB['U']).to_dataset(name='US')
        bbus['US'][:,:]=BB['U']
        bbvs = xr.zeros_like(BB['V']).to_dataset(name='VS')
        bbvs['VS'][:,:]=BB['V']
        bball = xr.merge([BB,bbus,bbvs]).chunk()
        bball.to_netcdf(fdfd[:-13]+'.s.'+fdfd[-13:])   
        os.remove(fdfd)
    return all_files,BB



import xarray as xr
from dask import delayed
from dask.diagnostics import ProgressBar

def make_nc_files_optimized(files_dict, Dateswanted, Dayswanted, FPout, prefix_out):
    """
    Optimized function to perform a specific task using Dask with specified resources.

    Parameters:
    - files_dict: A dictionary of files.
    - Dateswanted: List of dates.
    - Dayswanted: List of days.
    - FPout: Output file path.
    - prefix_out: Output file prefix.

    Returns:
    - delayed_writes: List of delayed write operations.
    """
    Static_zheight = xr.open_dataset('/glade/u/home/wchapman/RegriddERA5_CAMFV/static_operation_ERA5_zhght.nc')
    
    delayed_writes = []
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
        print('copying variables')
        DS['US'] = DS['U'].copy(deep=True)
        DS['VS'] = DS['V'].copy(deep=True)
        print('loaded')
        
        for ee, tt in enumerate(DS['time']):
            hourdo = DS['time.hour'][ee]
            datstr = str(dw)[:4] + str(dw)[5:7] + str(dw)[8:10] + f'{hourdo:02}'
            
            out_file_uvtq = FPout + '/' + prefix_out + '.uvtq.' + datstr + '.nc'
            delayed_write_uvtq = delayed(DS.sel(time=tt).squeeze().to_netcdf)(out_file_uvtq)
            delayed_writes.append(delayed_write_uvtq)
            
            out_file_ps = FPout + '/' + prefix_out + '.ps.' + datstr + '.nc'
            DS_ps['Z_GDS4_SFC'] = xr.zeros_like(DS_ps['SP'])
            DS_ps['Z_GDS4_SFC'][:, :] = Static_zheight['Z_GDS4_SFC'].values
            delayed_write_ps = delayed(DS_ps.sel(time=tt).squeeze().to_netcdf)(out_file_ps)
            delayed_writes.append(delayed_write_ps)

    # Compute the delayed write operations concurrently
    with ProgressBar():
        delayed_writes = list(dask.compute(*delayed_writes))

    return delayed_writes



def divide_datetime_index(date_index, max_items_per_division=4):
    """
    Divide a DatetimeIndex into sublists with a maximum number of items per division.

    Parameters:
    - date_index: DatetimeIndex to be divided.
    - max_items_per_division: Maximum number of items per division (default is 4).

    Returns:
    - divided_lists: List of sublists.
    """
    # Initialize an empty list to store the divided lists
    divided_lists = []

    # Initialize a sublist with the first date
    sublist = [date_index[0]]

    # Iterate through the remaining dates
    for date in date_index[1:]:
        # Add the current date to the sublist
        sublist.append(date)

        # Check if the sublist has reached the maximum allowed size
        if len(sublist) == max_items_per_division:
            # If it has, add the sublist to the divided_lists and reset the sublist
            divided_lists.append(sublist)
            sublist = []

    # If there are remaining items in the sublist, add it to the divided_lists
    if sublist:
        divided_lists.append(sublist)

    # Ensure that every division has at least two items by merging the last two divisions if necessary
    if len(divided_lists[-1]) < 2 and len(divided_lists) > 1:
        last_two_lists = divided_lists[-2:]  # Get the last two divisions
        combined_list = sum(last_two_lists, [])  # Combine them
        divided_lists = divided_lists[:-2]  # Remove the last two divisions
        divided_lists.append(combined_list)  # Add the combined list back

    return divided_lists

def increment_date_by_one_day(date_str):
    """
    Increment a date by one day and return it as a string.

    Parameters:
    - date_str: Input date string in the format 'YYYY-MM-DD'.

    Returns:
    - incremented_date_str: Date string incremented by one day.
    """
    # Convert the input date string to a pandas Timestamp
    date = pd.Timestamp(date_str)

    # Increment the date by one day
    incremented_date = date + pd.DateOffset(days=1)

    # Convert the incremented date back to a string in the same format
    incremented_date_str = incremented_date.strftime('%Y-%m-%d')

    return incremented_date_str


if __name__ == '__main__':
    #look at all the dates:
    Dayswantedtot = pd.date_range(start=start_date,end=end_date,freq=str(interval_hours)+'D')
    #look at all the dates:
    print(len(Dayswantedtot))
    if len(Dayswantedtot)<4:
        start_time = time.time()  # Record the start time
        Dayswanted = pd.date_range(start=start_date,end=end_date,freq=str(interval_hours)+'D')
        Dateswanted = pd.date_range(start=start_date,end=end_date,freq=str(interval_hours)+'H')
        Static_zheight = xr.open_dataset('/glade/u/home/wchapman/RegriddERA5_CAMFV/static_operation_ERA5_zhght.nc')
        files_dict=fp_dates_wanted(Dateswanted)
        #make the files:
        print('...starting processing...')
        delayed_writes = make_nc_files_optimized(files_dict, Dateswanted, Dayswanted,FPout, prefix_out)
        elapsed_time = time.time() - start_time
        print(f" executed in {elapsed_time} seconds")
    else: 
        print('in here!!')
        divided_lists =divide_datetime_index(Dayswantedtot)

        for dd in divided_lists:
            strtd = str(dd[0])[:10]
            endd  = str(dd[-1])[:10]
            endd  = increment_date_by_one_day(endd)
            print('doing files:',strtd,endd)
            start_time = time.time()  # Record the start time
            Dayswanted = pd.date_range(start=strtd,end=endd,freq=str(interval_hours)+'D')
            Dateswanted = pd.date_range(start=strtd,end=endd,freq=str(interval_hours)+'H')
            Static_zheight = xr.open_dataset('/glade/u/home/wchapman/RegriddERA5_CAMFV/static_operation_ERA5_zhght.nc')
            files_dict=fp_dates_wanted(Dateswanted)
            #make the files:
            print('...starting processing...')
            delayed_writes = make_nc_files_optimized(files_dict, Dateswanted, Dayswanted,FPout, prefix_out)
            elapsed_time = time.time() - start_time
            print(f" phase executed in {elapsed_time} seconds")
            
    
    if 'client' in locals():
        client.shutdown()
        print('...shutdown client...')
    else:
        print('client does not exist yet')
