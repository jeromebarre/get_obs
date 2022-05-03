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

conv_path = Path('$HOME/ioda-bundle/build/bin/')

class obs_win(object):
    def __init__(self, sta, end, win, pfm, ins, obv, pio, cln):
        self.sta = sta
        self.end = end
        self.win = win
        self.pfm = pfm
        self.ins = ins
        self.obv = obv
        self.pio = pio
        self.cln = cln
        self.tmpdir = Path(__file__).parent/str('tmp_'+self.pfm+'_'+self.ins+'_'+self.obv)
        self.get_win_range()
        if self.ins == 'MODIS': self.getnconv_modis() 
        

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

    def getnconv_modis(self):
        if self.pfm == "Terra": pref="MOD04_L2"
        if self.pfm == "Aqua": pref="MYD04_L2"

        tokfile = Path(__file__).parent/'eosdis_token'
        if not os.path.isfile(tokfile):
            tok = input("Enter token from https://ladsweb.modaps.eosdis.nasa.gov/: ")
            with open(tokfile, 'w') as f: f.write(tok)
        else:
            with open(tokfile, 'r') as f: tok = f.read()
        f.close()

        hdrp = ' --header "Authorization: Bearer ' +tok+'" '
        url = 'https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/'
        cmd = 'wget -e robots=off -m -nv -np --reject html,tmp -nH --cut-dirs=6 -A '
        exe = conv_path/'modis_aod2ioda.py '
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
            #$binioda/modis_aod2ioda.py -i tmp_Terra_MODIS_AOD/* -t 2021080100 -p Terra -o test.nc
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
    sta = ymlist["start date"]
    end = ymlist["end date"]
    win = Timedelta(ymlist["window length"])

    pfm = ymlist["platform"]
    ins = ymlist["instrument"]
    obv = ymlist["observable"] 

    pio = ymlist["path ioda"]
    cln = ymlist["clean"]

    owclass = obs_win(sta, end, win, pfm, ins, obv, pio, cln)

    

if __name__ == '__main__':
    main()
