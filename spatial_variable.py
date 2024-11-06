from glob import glob
class spatial_variable(object):

    def __init__(self):
    
       self.var_filetype = 'netcdf' # or 'hdf5
       self.variable = 'precipitation'
       self.folder_address = r"e:\data" # Save only the .nc files that need  to be 


    def get_spfiles_list(self):
           return [f_address for enum, f_address in enumerate(glob(self.folder_address + "/*.nc4"))]


