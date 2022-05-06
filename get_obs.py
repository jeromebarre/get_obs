#!/usr/bin/env python3

#
# (C) Copyright 2022 UCAR
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#

import sys
import argparse
import netCDF4 as nc
import numpy as np
from datetime import datetime, date #, timedelta
from pandas import Timedelta, date_range
import os
import shutil
from pathlib import Path
import yaml as ym

class obs_win(object):
    def __init__(self, sta, end, win, pfm, ins, obv, pio, pbd, cln, cch):
        self.sta = sta
        self.end = end
        self.win = win
        self.pfm = pfm
        self.ins = ins
        self.obv = obv
        self.pio = pio
        self.pbd = pbd
        version = sys.version.split('.')[0]+'.'+sys.version.split('.')[1]
        os.environ["PYTHONPATH"] = self.pbd+'/lib/python'+version+'/pyioda/ioda/../:/usr/local/lib'
        self.cln = cln
        self.cch = cch
        self.tmpdir = Path(__file__).parent/str('tmp_'+self.pfm+'_'+self.ins+'_'+self.obv)
        self.get_win_range()
        if self.ins == 'MODIS': self.getnconv_modis()
        if self.ins == 'VIIRS': self.getnconv_viirs()
        

    def get_win_range(self):
        self.lwin_s = date_range(start=self.sta, end=self.end, freq=self.win)
        self.lwin_e = date_range(start=self.sta+self.win, end=self.end+self.win, freq=self.win)

    def check_clean(self,finish=False):
        if self.cln and os.path.isdir(self.tmpdir):
            shutil.rmtree(self.tmpdir)
        if not os.path.isdir(self.tmpdir) and not finish:
            Path(self.tmpdir).mkdir()
        if finish: exit()
        if not os.path.isdir(self.pio):Path(self.pio).mkdir()

    def getnconv_viirs(self):
        '''
        obs ingest function for VIIRS NOAA archive product (not NRT)
        it will work with ordering the data first on
        https://www.avl.class.noaa.gov/saa/products/search?datatype_family=JPSS_GRAN
        and then using ftp path provided by NOAA CLASS
        e.g. ftp.avl.class.noaa.gov/<order#>/<suborder#>/001/
        '''
        if self.pfm == "NPP": pref = "npp"
        if self.pfm == "NOAA-20": pref = "j01"
        ordr_f = Path(__file__).parent/'order_file'
        if not os.path.isfile(ordr_f) or not self.cch:
            order = input("Enter order numbers provided by CLASS order"+ \
                    +"(next to the cd command on the order email):")
            with open(ordr_f, 'w') as f: f.write(order)
        else:
            with open(ordr_f, 'r') as f: order = f.read()
        f.close()
        #not sure for now
        order = order+'/001/'
        ftp = ' ftp://anonymous:psswd@ftp.avl.class.noaa.gov/'+order
        cmd = 'wget -r -nc -nd -np -nv -A '
        exe = Path(self.pbd)/'bin'/'viirs_aod2ioda.py '
        #get the files hourly for now (lazy coding)
        for w_s,w_e in zip(self.lwin_s,self.lwin_e):
            finish = False
            if w_s == self.lwin_s[-1]: finish = True
            self.check_clean(finish)
            w_c = w_s
            while w_c < w_e:
                w_c_p1 = w_c + Timedelta(hours=1)
                w_c_m1 = w_c + Timedelta(hours=-1)
                ymdh = w_c.strftime('%Y') + w_c.strftime('%m') + \
                        w_c.strftime('%d') + w_c.strftime('%H')
                ymdh_p1 = w_c_p1.strftime('%Y') + w_c_p1.strftime('%m') + \
                        w_c_p1.strftime('%d') + w_c_p1.strftime('%H')
                ymdh_m1 = w_c_m1.strftime('%Y') + w_c_m1.strftime('%m') + \
                        w_c_m1.strftime('%d') + w_c_m1.strftime('%H')

                fnam = ' "JRR-AOD_*_'+pref+'_s'+ymdh+'*_e'+ymdh+'*_c*.tar,'+    \
                          'JRR-AOD_*_'+pref+'_s'+ymdh_m1+'*_e'+ymdh+'*_c*.tar,'+ \
                          'JRR-AOD_*_'+pref+'_s'+ymdh+'*_e'+ymdh_p1+'*_c*.tar" '

                locf = ' -P '+str(self.tmpdir)

                fcmd = cmd + fnam + ftp + locf
                os.system(fcmd)
                w_c = w_c + Timedelta(hours=1)
            os.system('cat '+str(self.tmpdir)+'/*.tar | tar -xvf - -i -C '+str(self.tmpdir)+'/')
            w_m = w_s + self.win//2
            ymdh_m = w_m.strftime("%Y%m%d%H")
            fout = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh_m+'.nc'
            #thining set to 0.1 (10%) for space and speed
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/*.nc -n 0.1 -m nesdis -k maskout -o '+fout)



    def getnconv_modis(self):
        '''
        obs ingest function for MODIS NASA product
        on https://ladsweb.modaps.eosdis.nasa.gov
        '''
        if self.pfm == "Terra": pref="MOD04_L2"
        if self.pfm == "Aqua": pref="MYD04_L2"

        tokfile = Path(__file__).parent/'eosdis_token'
        if not os.path.isfile(tokfile) or not self.cch:
            tok = input("Enter token from https://ladsweb.modaps.eosdis.nasa.gov/: ")
            with open(tokfile, 'w') as f: f.write(tok)
        else:
            with open(tokfile, 'r') as f: tok = f.read()
        f.close()

        hdrp = ' --header "Authorization: Bearer ' +tok+'" '
        url = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/'
        cmd = 'wget -e robots=off -m -nc -nv -np --reject html,tmp -nH --cut-dirs=6 -A '
        exe = Path(self.pbd)/'bin'/'modis_aod2ioda.py '
        for w_s,w_e in zip(self.lwin_s,self.lwin_e):
            finish = False
            if w_s == self.lwin_s[-1]: finish = True 
            self.check_clean(finish)
            w_c = w_s
            while w_c < w_e:
                doy, yr, hr, mn = str(w_c.timetuple().tm_yday), w_c.strftime('%Y'), \
                        w_c.strftime('%H'), w_c.strftime('%M')
                furl = ' '+url+pref+'/'+yr+'/'+doy+'/ '
                fnam = ' "'+pref+'.A'+yr+doy+'.'+hr+mn+'.061.*.hdf" '
                locf = ' -P '+str(self.tmpdir) #+'/'+pref+'.A'+yr+doy+'.'+hr+mn+'.hdf '
                hdrp = ' --header "Authorization: Bearer ' +tok+'" '
                
                fcmd = cmd + fnam + hdrp + furl + locf
                os.system(fcmd)

                w_c = w_c + Timedelta(minutes=5)

            w_m = w_s + self.win//2
            ymdh = w_m.strftime("%Y%m%d%H")
            fout = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh+'.nc'
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/* -t '+ymdh+' -p '+self.pfm+' -o '+fout)



def main():

    parser = argparse.ArgumentParser(
        description=(
            'Dowload,make IODA files and clean at DA window times with yaml as input params: -i ')
    )

    required = parser.add_argument_group(title='required arguments')
    required.add_argument(
        '-i', '--yaml_file',
        help="yaml input file",
        type=str, required=True)

    args = parser.parse_args()
    ymlist = ym.load(open(args.yaml_file),Loader=ym.FullLoader)
    #ideally we would want to pass data structures to reduce args passed in class
    sta = ymlist["start date"]
    end = ymlist["end date"]
    win = Timedelta(ymlist["window length"])

    pfm = ymlist["platform"]
    ins = ymlist["instrument"]
    obv = ymlist["observable"] 

    pio = ymlist["path ioda out"]
    pbd = ymlist["path build"]

    cln = ymlist["clean"]
    cch = ymlist["cache"]

    owclass = obs_win(sta, end, win, pfm, ins, obv, pio, pbd, cln, cch)

    

if __name__ == '__main__':
    main()
