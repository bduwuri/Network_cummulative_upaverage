import geopandas as gpd
import pandas as pd
import math
class network_info:
    
    def __init__(self):

        self.all_pfs = [ 11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,34,35,36,41,42,43,44,45,46,47,48,49,51,52,53,54,55,56,57,61,62,63,64,65,66,67,71,72,73,74,75,76,77,78,81,81,82,83,84,85,86]

    def get_WID(self, file_num):

        region = math.floor(file_num/10)
        if region in [7,6,8,3,4]:
            path_wid = f"G:/.shortcut-targets-by-id/14U8nf4Xqs1-T_TzW0NUlLZKORqNHHdz3/GRACE/MERITHYDRO/region_{region}/{file_num}/"
            data_WID = pd.read_table(path_wid + "riv_output_WID.txt", header=0)
        elif region in [5,1,2,8,9]: 
            path_wid = f"G:/.shortcut-targets-by-id/14U8nf4Xqs1-T_TzW0NUlLZKORqNHHdz3/GRACE/MERITHYDRO/region_{region}/{file_num}"
            data_WID = pd.read_table(path_wid + "_riv_output_WID.txt", header=0)
        return data_WID
    
    def get_cat(self, file_num):

        region = math.floor(file_num/10)
        path_cat = 'C:/Users/duvvuri.b/OneDrive - Northeastern University/GRACE/GRACE/level-2/{}/'.format(file_num)
        data2 = gpd.read_file(path_cat + "cat_pfaf_{}_MERIT_Hydro_v07_Basins_v01_bugfix1.dbf".format(file_num))
        return data2

    def sanity_checknetwork(self,data2,data_WID):
            # Post processing of River network to remove lake polygons/catchments
            mask = data2['COMID'].astype('int').isin(data_WID['COMID'].astype('int'))
            data2 = data2[mask]
            mask = data_WID['COMID'].astype('int').isin(data2['COMID'].astype('int'))
            data_WID = data_WID[mask]
    
            # Rearrange cathments dataframe (COMIDS) to match order of cathments in data_WID
            data2 = data2.set_index('COMID')
            data2 = data2.reindex(index=data_WID['COMID'])
            data2 = data2.reset_index()

            try:
                assert data2['COMID'].shape[0] == data_WID['COMID'].shape[0], "The number of rows in data2 and data_WID do not match"
                assert (data2['COMID'] == data_WID['COMID']).all(), "The COMID columns in data2 and data_WID are not identical"
                return data2, data_WID
            except AssertionError as e:
                print(f"Sanity check failed: {str(e)}")
                return False


    def get_step4_code(self):

        r_code = '''
        library(data.table)
    
        calculate_upstream_average <- function(data_WID, df_step3, pf,varnames_col) {

          numrows <- nrow(data_WID)
          numTWSArows <- nrow(df_step3)
          numDTs <- as.integer(numTWSArows / numrows)
      
          RIVID <- data_WID$RIVID
          unitarea <- data_WID$unitarea
          chuparea <- data_WID$chuparea
          downID <- data_WID$downID

          varnames_col = varnames_col[2:dim(df_step3)[2]]
          cummulative_yr <- data.frame()
          t  = 1 
           {
            TWSA <- matrix(as.integer(0), nrow=numrows, ncol=length(varnames_col)+1)
            TWSA[,1] <- data_WID$RIVID
        
            for(k in 1:numrows) {
              tempTWSAarea <- round(as.numeric(df_step3[((t-1)*numrows + k),varnames_col]) * unitarea[k], 6)
          
              if (all(is.na(tempTWSAarea))) {
                downID_curr <- downID[k]
                curr_time <- df_step3[((t-1)*numrows + k), 'time']
                tempTWSAarea <- as.numeric(df_step3[(df_step3$RIVID == downID_curr), varnames_col]) * unitarea[k]
            
                if (downID_curr == 0) {
                  up_curr <- data_WID[data_WID$RIVID == RIVID[k], 'up1']
                  tempTWSAarea <- as.numeric(df_step3[(df_step3$RIVID == up_curr), varnames_col]) * unitarea[k]
                }
              }
          
              if(downID[k] > 0) {
                rivnum <- k
                nextdown <- downID[k]
                while(nextdown > 0) {
                  TWSA[rivnum, 2:(length(varnames_col) + 1)] <- TWSA[rivnum, 2:(length(varnames_col) + 1)] + tempTWSAarea
                  rivnum <- which(RIVID == nextdown)
                  nextdown <- downID[rivnum]
                }
                if(nextdown == 0) {
                  TWSA[rivnum, 2:(length(varnames_col) + 1)] <- TWSA[rivnum, 2:(length(varnames_col) + 1)] + tempTWSAarea
                }
              } else {
                TWSA[k, 2:(length(varnames_col) + 1)] <- TWSA[k, 2:(length(varnames_col) + 1)] + tempTWSAarea
              }
            }
            for(k in 1:numrows) {
              TWSA[k, 2:(length(varnames_col) + 1)] <- TWSA[k, 2:(length(varnames_col) + 1)] / chuparea[k]
            }
            TWSA <- as.data.frame(TWSA)
            colnames(TWSA) = c('RIVID',varnames_col)
        
          }
          #write.csv(TWSA,file=sprintf('E:/precep_processed_1105/step4_%d.csv',pf),row.names = FALSE)
          return (TWSA)
        }
        '''
        return r_code

