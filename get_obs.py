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
        os.environ["PYTHONPATH"] = self.pbd+'/lib/python'+version+':' \
                                   +os.environ["PYTHONPATH"]
        os.system('echo $PYTHONPATH')
        self.cln = cln
        self.cch = cch
        self.tmpdir = Path(__file__).parent/str('tmp_'+self.pfm+'_'+self.ins+'_'+self.obv)
        self.get_win_range()
        if self.ins == 'MODIS': self.getnconv_modis()
        if self.ins == 'VIIRS': self.getnconv_viirs()
        if self.ins == 'TROPOMI': self.getnconv_tropomi()
        if self.ins == 'MOPITT': self.getnconv_mopitt()
        if self.ins == 'TEMPO': self.getnconv_tempo()

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
                    "(next to the cd command on the order email):")
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
            #thining set to 0.9 (10%) for space and speed
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/*.nc -n 0.9 -m nesdis -k maskout -o '+fout)


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
        cmd = 'wget -e robots=off -r -nc -nd -np -nv -A '
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
                fnam = ' "'+pref+'.A'+yr+doy+'.'+hr+mn+'.061.*" '
                locf = ' -P '+str(self.tmpdir) #+'/'+pref+'.A'+yr+doy+'.'+hr+mn+'.hdf '
                hdrp = ' --header "Authorization: Bearer ' +tok+'" '

                fcmd = cmd + fnam + hdrp + furl + locf
                os.system(fcmd)

                w_c = w_c + Timedelta(minutes=5)

            w_m = w_s + self.win//2
            ymdh = w_m.strftime("%Y%m%d%H")
            fout = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh+'.nc'
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/* -t '+ymdh+' -p '+self.pfm+' -o '+fout)

    def getnconv_tropomi(self):
        import xml.etree.ElementTree as ET
        '''
        obs ingest function for TROPOMI products using the s5phub API
        on https://s5phub.copernicus.eu/
        '''

        exe = Path(self.pbd)/'bin'/'tropomi_no2_co_nc2ioda.py '

        xmlist='list_tropomi.xml'
        if os.path.exists(xmlist): os.remove(xmlist)
        #pass and id are being the same since 2018 and is kind of public, not sure this will change soon
        wgc = 'wget --user=s5pguest --password=s5pguest --no-check-certificate '
        apisearch='https://s5phub.copernicus.eu/dhus/search?q='
        dlurl='https://s5phub.copernicus.eu/dhus/odata/v1/Products'
        #other products could be added in the future
        if self.obv=='NO2':
            prod='L2__NO2___'; varname='no2'; qcthre='0.99'
            api_conf = 'producttype:'+prod
        if self.obv=='CO':
            mode = 'Reprocessing'
            prod='L2__CO____'
            varname='co'
            qcthre='0.99'
            api_conf = 'producttype:'+prod+' AND processingmode:'+mode

        for w_s,w_e in zip(self.lwin_s,self.lwin_e):
            finish = False
            if w_s == self.lwin_s[-1]: finish = True
            self.check_clean(finish)

            w_ss = w_s + Timedelta(hours=-1)
            ymdh_s = w_ss.strftime('%Y-%m-%dT%H') + ':00:00.000Z'
            ymdh_e = w_e.strftime('%Y-%m-%dT%H') + ':00:00.000Z'
            apis = apisearch+api_conf+' AND beginposition:['+ymdh_s+' TO '+ymdh_e+']'

            os.system(wgc+'--output-document='+xmlist+' "'+apis+'"')

            #get the uuids from the xml
            tree = ET.parse(xmlist)
            root = tree.getroot()
            for child1 in root:
                for child2 in child1:
                    if 'name' in child2.attrib and child2.attrib['name'] == 'uuid':
                       uuid = child2.text
                       durl=dlurl+"('"+uuid+"')/\$value"
                       durl='"'+durl+'"'
                       os.system('cd '+str(self.tmpdir)+';'+wgc+' --content-disposition '+durl+';cd ..')

            w_m = w_s + self.win//2
            ymdh_m = w_m.strftime('%Y%m%dT%H')
            fout_total = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh_m+'_'+self.obv+'_total.nc'
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/* -o '+fout_total+' -v '+varname+' -c total -n 0.9 -q '+qcthre)
            if self.obv=='NO2':
               fout_tropo = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh_m+'_'+self.obv+'_tropo.nc'
               os.system(str(exe)+'-i '+str(self.tmpdir)+'/* -o '+fout_tropo+' -v '+varname+' -c tropo -n 0.9 -q '+qcthre)

    def getnconv_mopitt(self):
        '''
        obs ingest function for MOPITT Terra product
        on https:///asdc.larc.nasa.gov/
        '''
        # could be passed as arguments
        ret_type = 'MOP02J'
        version = '008'

        tokfile = Path(__file__).parent/'earthdata_token'
        if not os.path.isfile(tokfile) or not self.cch:
            tok = input("Enter token from https:///asdc.larc.nasa.gov/: ")
            with open(tokfile, 'w') as f: f.write(tok)
        else:
            with open(tokfile, 'r') as f: tok = f.read()
        f.close()

        hdrp = ' --header "Authorization: Bearer ' +tok+'" '
        url = 'https://asdc.larc.nasa.gov/data/MOPITT/'+ret_type+'.'+version+'/'
        cmd = 'wget -e robots=off -r -nc -nd -np -nv -A '
        exe = Path(self.pbd)/'bin'/'mopitt_co_nc2ioda.py '
        for w_s,w_e in zip(self.lwin_s,self.lwin_e):
            finish = False
            if w_s == self.lwin_s[-1]: finish = True
            self.check_clean(finish)
            w_c = w_s
            wd_c = w_c.replace(hour=0, minute=0, second=0)
            wd_e = w_e.replace(hour=0, minute=0, second=0)
            while wd_c <= wd_e:
                yr, mo, dy = wd_c.strftime('%Y'), wd_c.strftime('%m'), wd_c.strftime('%d')
                furl = ' '+url+'/'+yr+'.'+mo+'.'+dy+'/ '
                locf = ' -P '+str(self.tmpdir)
                hdrp = ' --header "Authorization: Bearer ' +tok+'" '
                fnam = ' "'+ret_type+'-'+yr+mo+dy+'-L2V18.0.3.he5" '
                fcmd = cmd + fnam + hdrp + furl + locf
                print(fcmd)
                os.system(fcmd)

                wd_c = wd_c + Timedelta(days=1)

            w_m = w_s + self.win//2
            ymdh = w_m.strftime("%Y%m%d%H")
            fout = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh+'.nc'
            ymdh_s = w_s.strftime('%Y%m%d%H')
            ymdh_e = w_e.strftime('%Y%m%d%H')
            ymd_s = w_s.strftime('%Y%m%d')
            ymd_e = w_e.strftime('%Y%m%d')
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/*'+ymd_s+'* ' \
                                    +str(self.tmpdir)+'/*'+ymd_e+'* -r ' \
                                    +ymdh_s+' '+ymdh_e+' -o '+fout)

    def getnconv_tempo(self):
        '''
        obs ingest function for TEMPO proxy data
        on https:///asdc.larc.nasa.gov/
        '''
        ret_type = "L2"
        version = "V01"
        product = ret_type+"_"+version
        OBV = self.obv
        obv = self.obv.lower()

        hdrp = ' '
        url = ' /discover/nobackup/projects/gmao/geos_cf_dev/obs/TEMPO'
        cmd = 'cp -rf '
        exe = Path(self.pbd)/'bin'/'tempo_nc2ioda.py '
        for w_s,w_e in zip(self.lwin_s,self.lwin_e):
            finish = False
            if w_s == self.lwin_s[-1]: finish = True
            self.check_clean(finish)
            w_c = w_s
            print(w_s,w_c,w_e)
            while w_c < w_e:
                yr, mm, dd, hr = w_c.strftime('%Y'), w_c.strftime('%m'), w_c.strftime('%d'), \
                    w_c.strftime('%H')
                furl = ' '+url+'_'+product
                fnam = '/TEMPO_'+self.obv+'_'+product+'_'+yr+mm+dd+'T'+hr+'* '
                locf = ' '+str(self.tmpdir)

                fcmd = cmd + furl + fnam + hdrp + locf
                os.system(fcmd)
                os.system('ls '+locf)
                w_c = w_c + Timedelta(hours=1)

            w_m = w_s + self.win//2
            ymdh = w_m.strftime("%Y%m%d%H")
            fout = self.pio+'/'+self.ins+'_'+self.pfm+'_'+ymdh+'.nc'
            os.system(str(exe)+'-i '+str(self.tmpdir)+'/* -c troposphere -v '+obv+' -o '+fout)

def main():

    parser = argparse.ArgumentParser(
        description=(
            'Download, make IODA files and clean at DA window times with yaml as input params: -i ')
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
