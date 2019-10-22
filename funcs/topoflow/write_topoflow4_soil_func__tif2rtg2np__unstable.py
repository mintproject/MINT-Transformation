# Copyright (c) 2019, Scott D. Peckham, Binh Vu, Basel Shbita
# August - October 2019
# See:
# https://csdms.colorado.edu/wiki/Model_help:TopoFlow-Soil_Properties_Page

import os
from pathlib import Path
from typing import Union

import gdal  ## ogr
import numpy as np
from scipy.special import gamma

from dtran import IFunc, ArgType
import funcs.topoflow.rti_files as rti_files
import funcs.topoflow.write_topoflow4_climate_func as rg


class Topoflow4SoilWriteFunc(IFunc):
    id = "topoflow4_soil_write_func"
    description = ''' A reader-transformation-writer multi-adapter.
    Creates Bin (and RTI) files from tiff (soil) files.
    '''
    inputs = {
        "input_dir": ArgType.String,
        "output_dir": ArgType.FilePath,
        "layer": ArgType.String,
        "DEM_bounds": ArgType.String,
        "DEM_xres_arcsecs": ArgType.String,
        "DEM_yres_arcsecs": ArgType.String,
        "DEM_ncols": ArgType.String,
        "DEM_nrows": ArgType.String,
    }
    outputs = {}

    def __init__(self, input_dir: str, output_dir: Union[str, Path], layer: str, DEM_bounds: str, DEM_xres_arcsecs: str, DEM_yres_arcsecs: str,
                 DEM_ncols: str, DEM_nrows: str):
        self.DEM = {
            "bounds": [float(x.strip()) for x in DEM_bounds.split(",")],
            "xres": float(DEM_xres_arcsecs) / 3600.0,
            "yres": float(DEM_yres_arcsecs) / 3600.0,
            "ncols": int(DEM_ncols),
            "nrows": int(DEM_nrows),
        }
        self.input_dir = str(input_dir)
        self.output_dir = str(output_dir)
        self.layer = layer

    def exec(self) -> dict:
        save_soil_hydraulic_vars(input_dir=self.input_dir,
                                 output_dir=self.output_dir,
                                 DEM_info=self.DEM,
                                 layer=self.layer)
        return {}

    def validate(self) -> bool:
        return True

# -------------------------------------------------------------------
def save_soil_hydraulic_vars(input_dir, output_dir, DEM_info: dict, layer=1):
              
    (C, S, OM, D) = read_soil_grid_files(input_dir, DEM_info, layer=layer)

    topsoil = (layer == 1)
    subsoil = not(topsoil)
    
    (theta_s, K_s, alpha, n, L) = get_wosten_vars(C, S, OM, D, topsoil, subsoil)   
    (psi_B, c, lam, eta, G ) = get_tBC_from_vG_vars(alpha, n, L)
     
    #---------------------------------   
    # Basic soil hydraulic variables
    #---------------------------------
    Ks_file   = output_dir + '_2D-Ks.bin'
    qs_file   = output_dir + '_2D-qs.bin'
    pB_file   = output_dir + '_2D-pB.bin'
    #-------------------------------------------
    # The transitional Brooks-Corey parameters
    #-------------------------------------------
    c_file    = output_dir + '_2D-c.bin'    
    lam_file  = output_dir + '_2D-lam.bin'
    G_file    = output_dir + '_2D-G.bin'
    ## eta_file  = output_dir + '_2D-eta.bin'
    # c_file    = output_dir + '_2D-tBC-c.bin'    
    # lam_file  = output_dir + '_2D-tBC-lam.bin'
    # G_file    = output_dir + '_2D-tBC-G.bin'
    ## eta_file  = output_dir + '_2D-tBC-eta.bin'
    #-------------------------------
    # The van Genuchten parameters
    #-------------------------------
    a_file    = output_dir + '_2D-vG-alpha.bin'
    n_file    = output_dir + '_2D-vG-n.bin'
    L_file    = output_dir + '_2D-vG-L.bin'

    #-------------------------------    
    # Write all variables to files
    #-------------------------------
    Ks_unit = open(Ks_file, 'wb')
    K_s = np.float32( K_s )  
    K_s.tofile( Ks_unit )
    Ks_unit.close()
    #----------------------------------
    qs_unit = open(qs_file, 'wb')
    theta_s = np.float32( theta_s )
    theta_s.tofile( qs_unit )
    qs_unit.close()
    #----------------------------------
    pB_unit = open(pB_file, 'wb')
    psi_B   = np.float32( psi_B )
    psi_B.tofile( pB_unit)
    pB_unit.close()
    #----------------------------------
    c_unit = open(c_file, 'wb')
    c = np.float32( c )
    c.tofile( c_unit )
    c_unit.close()
    #----------------------------------
    lam_unit = open(lam_file, 'wb')
    lam = np.float32( lam )
    lam.tofile( lam_unit)
    lam_unit.close()
    #----------------------------------
    G_unit = open(G_file, 'wb')
    G = np.float32( G )
    G.tofile( G_unit )
    G_unit.close()
    #----------------------------------
    # eta_unit = open(eta_file, 'wb')
    # eta = np.float32( eta )
    # eta.tofile( eta_unit )
    # eta_unit.close()
    #----------------------------------
    a_unit = open(a_file, 'wb')
    alpha = np.float32( alpha )
    alpha.tofile( a_unit )
    a_unit.close()
    #----------------------------------
    n_unit = open(n_file, 'wb')
    n = np.float32( n )
    n.tofile( n_unit)
    n_unit.close()
    #----------------------------------
    L_unit = open(L_file, 'wb')
    L = np.float32( L )
    L.tofile( L_unit )
    L_unit.close()

    for fpath in [Ks_file, qs_file, pB_file, c_file, lam_file, G_file, a_file, n_file, L_file]:
        rti_files.generate_rti_file(fpath, fpath.replace(".bin", ".rti"), DEM_info["ncols"],
                                   DEM_info['nrows'], DEM_info["xres"], DEM_info['yres'], pixel_geom=0)

# -------------------------------------------------------------------
def read_soil_grid_files(input_dir, DEM_info: dict,
                         res_str='1km', layer=1, VERBOSE=True):

    #-------------------------------------------------------------
    # Read soil property data from a set TIF files, clip and resample
    # to a DEM grid to be used by TopoFlow.
    # These basic soil properties are needed to compute several
    # soil hydraulic properties using the pedotransfer functions
    # (PTFs) in Wosten (1998).
    #-------------------------------------------------------------
    # Read percent clay
    # Read percent silt
    # Read percent organic matter
    # Read bulk density
    #-------------------------------------------------------------
    # Values are available for 7 different depths:
    #   0.0, 0.05, 0.15, 0.30, 0.60, 1.00, 2.00
    #   sl1, sl2,  sl3,  sl4,  sl5,  sl6,  sl7
    #--------------------------------------------------- 
    layer_str = str(layer)
    match_files = []
    for fpath in Path(input_dir).iterdir():
        if fpath.name.find(f"_M_sl{layer_str}_{res_str}") != -1 and (fpath.name.endswith(".tiff") or fpath.name.endswith(".TIF")):
            match_files.append(fpath)

    C_file_pre = [str(x) for x in match_files if x.name.find("CLYPPT_") != -1][0]
    S_file_pre = [str(x) for x in match_files if x.name.find("SLTPPT_") != -1][0]
    OM_file_pre = [str(x) for x in match_files if x.name.find("ORCDRC_") != -1][0]
    D_file_pre = [str(x) for x in match_files if x.name.find("BLDFIE_") != -1][0]

    print([C_file_pre, S_file_pre, OM_file_pre, D_file_pre])

    # Transform to RTG.
    [C_file, S_file, OM_file, D_file] = transform_isric_soil_grid_files([C_file_pre, S_file_pre, OM_file_pre, D_file_pre], DEM_info)
    # These grids now contain data from ISRIC - SoilGrids, but have
    # been clipped and resampled to a DEM grid to be used by TopoFlow.
    
    print([C_file, S_file, OM_file, D_file])

    #-------------------------------
    # Read the soil property grids
    #------------------------------------------------
    # Wosten C = clay mass fraction, [kg/kg], as %.
    # Same units in ISRIC and Wosten.
    #------------------------------------------------
    C = np.fromfile(open(C_file, 'rb'), dtype=np.float32)

    #------------------------------------------------
    # Wosten S = silt mass fraction, [kg/kg], as %.
    # Same units in ISRIC and Wosten.
    #------------------------------------------------    
    S = np.fromfile(open(S_file, 'rb'), dtype=np.float32)
    #----------------------------------------------------------
    # Wosten OM = organic matter mass fraction, [kg/kg], as %
    # ISRIC units = [g / kg]  
    # Convert ISIRC [g / kg] to Wosten [%].
    #--------------------------------------------------
    # First convert [g/kg] to [kg/kg] (mass fraction),
    # Then multiply by 100 to convert [kg/kg] to %.
    # Note:  mass fraction [kg/kg] is in [0,1].
    #--------------------------------------------------
    # OM [g/kg] * (1 kg / 1000 g) = (OM/1000) [kg/kg]
    # (OM/1000) [kg/kg] * 100 = (OM/10) %.
    #--------------------------------------------------
    OM = np.fromfile(open(OM_file, 'rb'), dtype=np.float32)
    OM = (OM / 10.0)
    #--------------------------------------
    # Wosten D = bulk density, [g / cm^3]
    #-------------------------------------------------
    # Convert ISRIC [kg / m^3] to Wosten [g / cm^3]
    #-------------------------------------------------
    # D [kg / m^3] * (1000 g / 1 kg) * (1 m / 100 cm)^3
    # D [kg / m^3] * (1 / 1000) = D [g / cm^3]
    #----------------------------------------------------
    D = np.fromfile(open(D_file, 'rb'), dtype=np.float32)
    D = (D / 1000.0)

    #--------------------------------------
    # Check if grid values are reasonable
    #--------------------------------------
    w1 = np.logical_or(C < 0, C > 100.0)
    n1 = w1.sum()
    if (n1 > 0):
        cmin = C.min()
        cmax = C.max()
        print('WARNING in read_soil_grid_files:')
        print('   Some values in C grid are out of range.')
        print('   min(C) = ' + str(cmin) )
        print('   max(C) = ' + str(cmax) )
        print()
    #--------------------------------------------------------
    w2 = np.logical_or(S < 0, S > 100.0)
    n2 = w2.sum()
    if (n2 > 0):
        Smin = S.min()
        Smax = S.max()
        print('WARNING in read_soil_grid_files:')
        print('   Some values in S grid are out of range.')
        print('   min(S) = ' + str(Smin) )
        print('   max(S) = ' + str(Smax) )
        print()
    #--------------------------------------------------------
    w3 = np.logical_or(OM < 0, OM > 100.0)
    n3 = w3.sum()
    if (n3 > 0):
        OMmin = OM.min()
        OMmax = OM.max()
        print('WARNING in read_soil_grid_files:')
        print('   Some values in OM grid are out of range.')
        if (OMmin < 0):
            print('   min(OM) = ' + str(OMmin) )
            print('   Possible nodata value.')
        if (OMmax > 100):
            print('   max(OM) = ' + str(OMmax) )
            ## print('   Possible nodata value.')
        print()
    #-------------------------------------------------------- 
    w4 = np.logical_or(D < 0, D > 2.65)
    n4 = w4.sum()
    if (n4 > 0):
        Dmin = D.min()
        Dmax = D.max()
        print('WARNING in read_soil_grid_files:')
        print('   Some values in D grid are out of range.')
        if (Dmin < 0):
            print('   min(D) = ' + str(Dmin) )
            print('   Possible nodata value.')
        if (Dmax > 2.65):
            print('   max(D) = ' + str(Dmax) )
            ## print('   Possible nodata value.')
        print()

    #-------------------------------------------------
    # Replace zero values with another nodata value.
    # Zero values cause NaN or Inf in Wosten vars.
    # For %, 101.0 is an impossible value.
    # For D, use 10 [g / cm^3].
    #-------------------------------------------------
    # Density of water          = 1.00  [g / cm^3]
    # Density of silt           = 1.33  [g / cm^3]
    # Density of loose sand     = 1.442 [g / cm^3]
    # Density of dry sand       = 1.602 [g / cm^3]
    # Density of compacted clay = 1.746 [g / cm^3]
    # Density of most rocks     = 2.65  [g / cm^3]
    # Density of pure iron      = 7.87  [g / cm^3]
    # Density of pure actinium  = 10.07 [g / cm^3]  
    # Density of pure lead      = 11.34 [g / cm^3]
    # Density of pure gold      = 19.20 [g / cm^3]
    #-------------------------------------------------
    C[ C <= 0 ]   = 101.0  # [%]
    S[ S <= 0 ]   = 101.0  # [%]
    OM[ OM <= 0 ] = 101.0  # [%]
    D[ D <= 0]    = 10.0   # [g / cm^3]
    #---------------------------------------
    # C[ C <= 0 ]   = 0.001  # [%]
    # S[ S <= 0 ]   = 0.001  # [%]
    # OM[ OM <= 0 ] = 0.001  # [%]
    # D[ D <= 0]    = 0.001  # [g / cm^3]
      
    #------------------------------------
    # Return Wosten input vars as tuple
    #------------------------------------   
    return (C, S, OM, D)

def transform_isric_soil_grid_files(list_of_files, DEM_info: dict, VERBOSE=True):

    n_grids = 0
    bad_box_count = 0
    out_nodata = -9999.0
    tmp_file = '/tmp/TEMP.tif'

    list_of_transformed_files = []

    results = []
    for file in list_of_files:
        f = gdal.Open(file)

        # grid1 = f.ReadAsArray()
        band  = f.GetRasterBand(1)
        tiff_nodata = band.GetNoDataValue()

        # if (VERBOSE):
        #     print(f'===============================================================')
        #     print(f'ISRIC File = {file}')
        #     print(f'===============================================================')
        #     print(f'grid1: min   = {grid1.min()}, max = {grid1.max()}')
        #     print(f'grid1.shape  = {grid1.shape}')
        #     print(f'grid1.dtype  = {grid1.dtype}')

        #-----------------------------------------------        
        # Check if the bounding boxes actually overlap
        #-----------------------------------------------
        bad_box = False
        f_bounds = rg.get_raster_bounds(f, VERBOSE=VERBOSE)
        if (rg.bounds_disjoint(f_bounds, DEM_info["bounds"])):
            print(f'###############################################')
            print(f'WARNING: Bounding boxes do not overlap.')
            print(f'         New grid will contain only nodata.')
            print(f'###############################################')
            print(f'ds_bounds  = {f_bounds}')
            print(f'DEM_bounds = {DEM_info["bounds"]}\n')
            bad_box_count += 1
            bad_box = True

        #-------------------------------------------
        # Clip and resample data to the DEM's grid
        # then save to a temporary GeoTIFF file.
        #-------------------------------------------
        if not(bad_box):
            grid2 = rg.gdal_regrid_to_dem_grid(f, tmp_file,
                        out_nodata, DEM_info['bounds'], DEM_info['xres'], DEM_info['yres'],
                        RESAMPLE_ALGO='bilinear')
            if (VERBOSE):
                print(f'grid2: min  = {grid2.min()}, max = {grid2.max()}')
                print(f'grid2.shape = {grid2.shape}')
                print(f'grid2.dtype = {grid2.dtype}')
        else:
            grid2 = np.zeros((DEM_info['nrows'], DEM_info['ncols']), dtype='float32')
            grid2 += out_nodata

        del f

        #--------------------------------  
        # Write grid to new output file
        #------------------------------------
        grid2 = np.float32(grid2)
        out_file = Path(file).parent / (Path(file).stem + '.rtg')
        list_of_transformed_files.append(out_file)
        out_unit = open(out_file, 'wb')
        grid2.tofile(out_unit)
        out_unit.close()
        n_grids += 1

    print(f'Finished transforming ISRIC soil grid files.')
    print(f'   Number of grids = {str(n_grids)}')
    print(f'   Number outside of model domain = {str(bad_box_count)}')

    return list_of_transformed_files

# -------------------------------------------------------------------
def wosten_theta_s( C, S, OM, D, topsoil, subsoil ):

    #--------------------------------    
    # From Wosten (1998). R^2 = 76%
    #-------------------------------------------------------
    # Note that theta_s should be in [0, 1], unitless
    #-------------------------------------------------------
    # Equations in Wosten (1998), Wosten et al. (2001) and
    # Matula et al. (2007) all agree.
    #-------------------------------------------------------
    p1 = 0.7919 + 0.001691*C - 0.29619*D
    p2 = -0.000001491*S**2 + 0.0000821*OM**2  
    p3 = 0.02427 * (1/C) + 0.01113 * (1/S) + 0.01472 * np.log(S)
    p4 = (-0.0000733 * OM * C) - (0.000619 * D * C)
    p5 = (-0.001183 * D * OM) - (0.0001664 * topsoil * S)

    theta_s = (p1 + p2 + p3 + p4 + p5)
    
    #--------------------------------------
    # Check if grid values are reasonable
    #--------------------------------------
    w1 = np.logical_or( theta_s < 0.0, theta_s > 1.0 )
    n1 = w1.sum()
    if (n1 > 0):
        qmin = theta_s.min()
        qmax = theta_s.max()
        print('ERROR in wosten_theta_s:')
        print('   Some values are not in [0, 1].')
        if (qmin < 0.0):
            print('   min(theta_s) = ' + str(qmin) )
        if (qmax > 1.0):
            print('   max(theta_s) = ' + str(qmax) )
        print()

    return theta_s

# -------------------------------------------------------------------
def wosten_K_s( C, S, OM, D, topsoil, subsoil ):

    #-------------------------------------   
    # From Wosten (1998). R^2 = 19%.
    # K_s^* = ln(K_s), K_s > 0.
    # Units in Wosten (1998) are cm/day.
    # Note that K_s should be > 0.
    #-------------------------------------
    
    ######################################################
    # NOTE!!  In term p5, the coefficient is given by:
    #         0.02986 in Wosten (1998) and:
    #         0.2986 in Wosten et al. (2001).
    ######################################################
    p1 = 7.755 + 0.0352*S + 0.93*topsoil
    p2 = -0.967*D**2 - 0.000484*C**2 - 0.000322*S**2
    p3 = (0.001 / S) - (0.0748 / OM) - 0.643*np.log(S)
    p4 = (-0.01398 * D * C) - (0.1673 * D * OM)
    #----------------
    # Wosten (1998)
    #----------------
    p5 = (0.02986 * topsoil * C) - 0.03305 * topsoil * S
    #-----------------------    
    # Wosten et al. (2001)
    #-----------------------
    ### p5 = (0.2986 * topsoil * C) - 0.03305 * topsoil * S
    
    Ks =  np.exp( p1 + p2 + p3 + p4 + p5 )  # [cm /day]

    #--------------------------------------
    # Check if grid values are reasonable
    #----------------------------------------------------------------
    # A very high value of K_s is 10 [cm / s] or 0.1 [m / s] which
    # equals 864,000 [cm / day] or 8.64 [km / day].  K_s is usually
    # much smaller and ranges over many orders of magnitude.
    #----------------------------------------------------------------
    Ks_max = 864000.0
    w1 = np.logical_or( Ks < 0.0, Ks > Ks_max )
    n1 = w1.sum()
    if (n1 > 0):
        Ksmin = Ks.min()
        Ksmax = Ks.max()
        print('ERROR in wosten_K_s:')
        print('   Some values are out of range.')
        if (Ksmin < 0.0):
            print('   min(K_s) = ' + str(Ksmin) )
        if (Ksmax > Ks_max):
            print('   max(K_s) = ' + str(Ksmax) )
        print()
                    
    #--------------------------------------------
    # Convert units from cm/day to m/sec for TF
    #--------------------------------------------
    #  cm       1 m         1 day         1 hr
    #  ---  *  -------  *  -------  *  ----------
    #  day     100 cm       24 hr       3600 sec
    #----------------------------------------------
    Ks = Ks / (100 * 24.0 * 3600.0)   # [m/sec]
      
    return Ks

# -------------------------------------------------------------------
def wosten_alpha( C, S, OM, D, topsoil, subsoil):

    #---------------------------------   
    # From Wosten (1998). R^2 = 20%.
    # a^* = ln(a), a > 0.
    # Units are [1 / cm] ??    ###############  CHECK
    #---------------------------------
    p1 = -14.96 + 0.03135*C + 0.0351*S + 0.646*OM + 15.29*D
    p2 = -0.192*topsoil - 4.671*D**2 - 0.000781*C**2 - 0.00687*OM**2
    #-----------------------------------------
    # Wosten (1998) and Matula et al. (2007)
    #-----------------------------------------
    p3 = (0.0449 / OM) + 0.0663*np.log(S) + 0.1482*np.log(OM)
    p4 = (-0.04546 * D * S) + (0.4852 * D * OM)
    #-----------------------    
    # Wosten et al. (2001)
    #-----------------------
    ## p3 = (0.449 / OM) + 0.0663*np.log(S) + 0.1482*np.log(OM)
    ## p4 = (-0.4546 * D * S) - (0.4852 * D * OM)
    p5 = 0.00673 * topsoil * C

    #---------------------------------------------------------   
    # Wosten formula returns |alpha|, so multiply by -1.
    # The pressure head is negative in the unsaturated zone,
    # zero at the water table and positive below that.
    # alpha < 0 is also needed so that psi_B = 1/alpha < 0
    # and G > 0.
    #---------------------------------------------------------  
    alpha = -1.0 * np.exp( p1 + p2 + p3 + p4 + p5)
    
    #--------------------------------------
    # Check if grid values are reasonable
    #-----------------------------------------------------
    # psi_B values ranges from about -90 to -9 cm.
    # If alpha = 1 / psi_B, alpha in [-1/9, -1/90].
    #-----------------------------------------------------
    alpha_min = -1.0 / 9.0
    alpha_max = -1.0 / 90.0
    w1 = np.logical_or( alpha < alpha_min, alpha > alpha_max )
    n1 = w1.sum()
    if (n1 > 0):
        amin = alpha.min()
        amax = alpha.max()
        print('ERROR in wosten_alpha:')
        print('   Some values are out of range.')
        if (amin < alpha_min):
            print('   min(alpha) = ' + str(amin) )
        if (amax > alpha_max):
            print('   max(alpha) = ' + str(amax) )
        print()
        
    #-----------------------------------------------       
    # Convert units from [1/cm] to [1/m].
    # X (1/cm) * (100 cm / m) = 100 * x (1/m)
    #-------------------------------------------------
    # Note pressure heads, psi, considered by Wosten
    # are in the range:  0 to -16,000 cm.
    # Permanent wilting point = -15,000 cm.
    # Hygroscopic condition   = -31,000 cm.
    # Field capacity is at -340 cm
    #   (at which water can be held against gravity)
    #-------------------------------------------------  
    alpha *= 100.0  # [1/m]
    
    return alpha

# -------------------------------------------------------------------
def wosten_n( C, S, OM, D, topsoil, subsoil ):

    #------------------------------------------   
    # From Wosten (1998). R^2 = 54%.
    # n^* = ln(n-1), n > 1, unitless.
    # Wosten (1998) assumes that m = 1 - 1/n.
    #-------------------------------------------------------------
    # Equations in Wosten (1998) and Wosten et al. (2001) agree.
    #-------------------------------------------------------------
    # In Matula et al. (2007), p2 has: (0.002885 * OM)
    #-------------------------------------------------------------
    p1 = -25.23 - 0.02195*C + 0.0074*S - 0.1940*OM + 45.5*D
    p2 = -7.24*D**2 + 0.0003658*C**2 + 0.002885*OM**2 - (12.81 / D)
    p3 = (-0.1524 / S) - (0.01958 / OM) - 0.2876 * np.log(S)
    p4 = (-0.0709 * np.log(OM)) - (44.6 * np.log(D))
    p5 = (-0.02264 * D * C) + (0.0896 * D * OM) + (0.00718 * topsoil * C)
    
    n = (np.exp(p1 + p2 + p3 + p4 + p5) + 1)

    #--------------------------------------
    # Check if grid values are reasonable
    #--------------------------------------
    w1 = (n < 1.0)
    ## w1 = np.logical_or( n < 1.0, n > ????? )
    n1 = w1.sum()
    if (n1 > 0):
        nmin = n.min()
        nmax = n.max()
        print('ERROR in wosten_n:')
        print('   Some values are out of range.')
        print('   min(n) = ' + str(nmin) )
        print('   max(n) = ' + str(nmax) )
        print()
                
    return n

# -------------------------------------------------------------------
def wosten_L( C, S, OM, D, topsoil, subsoil ):

    #-------------------------------------------------------    
    # From Wosten (1998). R^2 = 12%. 
    # L^* = ln[(L+10)/(10-L)], -10 < L < +10, unitless.
    # Mualem (1976) says L should be about 0.5 on average.
    # Do modern van Genuchten formulas assume L = 1/2 ??
    # See:  Wosten et al. (2001) for more about L.
    #-------------------------------------------------------------
    # Equations in Wosten (1998) and Wosten et al. (2001) agree.
    #-------------------------------------------------------------
    p1 = 0.0202 + 0.0006193*C**2  - 0.001136*OM**2
    p2 = -0.2316 * np.log(OM) - (0.03544 * D * C)
    p3 = (0.00283 * D * S) + (0.0488 * D * OM)
    
    s1 = (p1 + p2 + p3)
    L = 10 * (np.exp(s1) - 1)/(np.exp(s1) + 1)

    #--------------------------------------
    # Check if grid values are reasonable
    #-----------------------------------------------------
    # Wosten constrains L to be in [-10, 10]
    #-----------------------------------------------------
    w1 = np.logical_or( L < -10, L > 10 )
    n1 = w1.sum()
    if (n1 > 0):
        print('ERROR in wosten_L:')
        print('   Some values are out of range.')
    
    return L

# -------------------------------------------------------------------
def get_wosten_vars(C, S, OM, D, topsoil, subsoil):

    #----------------------------------------------------------
    # Use the Wosten (1998) pedotransfer functions to compute
    # theta_s, K_s, and van Genuchten parameters, then save
    # them to files.
    #----------------------------------------------------------
    theta_s = wosten_theta_s( C, S, OM, D, topsoil, subsoil )
    K_s     = wosten_K_s( C, S, OM, D, topsoil, subsoil )
    alpha   = wosten_alpha( C, S, OM, D, topsoil, subsoil )
    n       = wosten_n( C, S, OM, D, topsoil, subsoil )
    L       = wosten_L( C, S, OM, D, topsoil, subsoil )
    
    return (theta_s, K_s, alpha, n, L)

# -------------------------------------------------------------------
def get_tBC_from_vG_vars( alpha, n, L ):

    #--------------------------------------------------------
    # Convert van Genuchten parameters to those of the
    # transitional Brooks-Corey model.  Although K
    # is computed quite differently for the transitional
    # Brooks-Corey and van Genuchten methods, the equations
    # for ψ are the same if we use the formulas below.
    # For more information, see:
    # https://csdms.colorado.edu/wiki/
    #   Model_help:TopoFlow-Soil_Properties_Page   
    #---------------------------------------------------------
    # NOTE: L is often fixed at 1/2, but Wosten lets it vary.
    # See p. 51 in Wosten (1998).
    # Also see p. 1 in Schaap and van Genuchten (2005).
    #---------------------------------------------------------
    # These equations appear to be general:
    #---------------------------------------------------------
    # (1)  m      = (1 - 1/n)
    # (1') n      = 1 / (1 - m)
    # (1") n-1    = m / (1 - m)
    # (2)  eta    = 2 + (3 * lambda)
    #---------------------------------------------------------
    # These equations come from forcing the transitional
    # Brooks-Corey and van Genuchten equations for pressure
    # head (psi) to match exactly (eta is not involved):
    # tBC params = psi_B, c, lambda  (and eta)
    # vG  params = alpha, m, n, L
    #---------------------------------------------------------
    # NOTE:  We only need alpha and n (not L) to set the
    #        transitional Brooks-Corey parameters.  We
    #        cannot make the functional forms match for K.
    #---------------------------------------------------------
    # (3) psi_B    = 1 / alpha_g   (both < 0)
    # (4) c        = n             (both > 1)
    # (5) lambda   = m * c = m * n    (Note: c/lambda = 1/m)
    #
    # Using (4) and (5) and (1):
    # (6) lambda = m * c = (1 - 1/n)*c = (1 - 1/n)*n = (n-1)
    #
    # Using (6) and (2)
    # eta    = 2 + (3*lambda) = 2 + 3*(n-1) = 3*n - 1
    # eta    = 3/(1-m) - 1 = [3 - (3-m)]/(1-m) = (2+m)/(1-m)
    #---------------------------------------------------------
    # (n > 1) => 0 < m < 1  
    # (n > 1) => (lambda > 0)
    # (n > 1) => (eta > 2)
    #---------------------------------------------------------  
    psi_B  = (1.0 / alpha)
    c      = n
    lam    = (n - 1)
    eta    = 3*n - 1
    
    #------------------------------------------
    # Compute the Green-Ampt parameter, G > 0
    #------------------------------------------
    # G = capillary length scale > 0
    # psi_B = bubbling pressure head < 0
    # Both have same units of:  ?????????
    #------------------------------------------
    G = -psi_B * gamma(1 + 1/c) * gamma((eta-1)/c) / gamma(eta/c)
 
    #--------------------------------------
    # Check if grid values are reasonable
    #--------------------------------------------------
    # See Peckham_et_al (2017) GPF paper on TopoFlow.
    #--------------------------------------------------
    # Theoretical results:
    #  G[c, eta] is in [0, -2 * psi_B].
    #  Definitions imply:  eta > 2, c > 3.
    #  Ignoring the coefficient in G[c,eta] above:
    #     Limit[ g[c,eta], c-> Infinity] = (eta/(eta-1)).
    #     For eta =2, this equals 2.
    #------------------------------------------------------
    w1 = np.logical_or( G < 0, G > -2*psi_B )
    n1 = w1.sum()
    if (n1 > 0):
        Gmin = G.min()
        Gmax = G.max()
        print('WARNING in get_tBC_from_vG_vars:')
        print('   Some values in G grid are out of range.')
        print('   min(G) = ' + str(Gmin) )
        print('   max(G) = ' + str(Gmax) )
        print()
           
    return (psi_B, c, lam, eta, G)