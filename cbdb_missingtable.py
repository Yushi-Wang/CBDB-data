# -*- coding: utf-8 -*-
"""
Created on Fri Jan  4 22:26:10 2019

@author: leo
"""

import numpy as np
import pandas as pd
import sys
import re
import random
import datetime
import calendar
from os import mkdir
main_path = "D:/learning/Arash/CBDB/"
try:
    mkdir(main_path)
except FileExistsError:
    pass

cbdb_kin_id=pd.read_csv(main_path+'output/cbdb_kin_id.csv')
cbdb_year_status=pd.read_csv(main_path+'output/cbdb_year_status.csv')
cbdb_year_status_actyear=pd.read_csv(main_path+'output/cbdb_year_status_actyear.csv')

cbdb_year_status['c_index_year']=cbdb_year_status['c_index_year'].replace(0,np.nan)
cbdb_year_status['c_birthyear']=cbdb_year_status['c_birthyear'].replace(0,np.nan)
cbdb_year_status['c_deathyear']=cbdb_year_status['c_deathyear'].replace(0,np.nan)
####number of observations
all_num_ob=len(np.unique(cbdb_year_status['c_personid']))
####percent of index year information
index_year=cbdb_year_status['c_index_year'].groupby(cbdb_year_status['c_personid'])
index_y=index_year.count()
num_index_y=index_y[index_y.isin([0])]
num_index_y=len(num_index_y)
percent_index_y=1-num_index_y/all_num_ob
percent_index_y=format(percent_index_y, '.2%')
####percent of birth year information
birth_year=cbdb_year_status['c_birthyear'].groupby(cbdb_year_status['c_personid'])
birth_y=birth_year.count()
num_birth_y=birth_y[birth_y.isin([0])]
num_birth_y=len(num_birth_y)
percent_birth_y=1-num_birth_y/all_num_ob
percent_birth_y=format(percent_birth_y, '.2%')
####percent of birth year information
death_year=cbdb_year_status['c_deathyear'].groupby(cbdb_year_status['c_personid'])
death_y=death_year.count()
num_death_y=death_y[death_y.isin([0])]
num_death_y=len(num_death_y)
percent_death_y=1-num_death_y/all_num_ob
percent_death_y=format(percent_death_y, '.2%')
####percent of any year information
cbdb_year_status_actyear['c_index_year']=cbdb_year_status_actyear['c_index_year'].fillna(0)
cbdb_year_status_actyear['c_birthyear']=cbdb_year_status_actyear['c_birthyear'].fillna(0)
cbdb_year_status_actyear['c_deathyear']=cbdb_year_status_actyear['c_deathyear'].fillna(0)
cbdb_year_status_actyear['c_firstyear']=cbdb_year_status_actyear['c_firstyear'].fillna(0)
cbdb_year_status_actyear['c_lastyear']=cbdb_year_status_actyear['c_lastyear'].fillna(0)
def function(a,b,c,d,e):
    if a!=0:
        return a
    elif b!=0:
        return b
    elif c!=0:
        return c
    elif d!=0:
        return d
    else:
        return e
cbdb_year_status_actyear['year']=cbdb_year_status_actyear.apply(lambda x: function(x.c_index_year,x.c_birthyear,x.c_deathyear,x.c_firstyear,x.c_lastyear),axis=1)
cbdb_year_status_actyear['c_index_year']=cbdb_year_status_actyear['c_index_year'].replace(0,np.nan)
cbdb_year_status_actyear['c_birthyear']=cbdb_year_status_actyear['c_birthyear'].replace(0,np.nan)
cbdb_year_status_actyear['c_deathyear']=cbdb_year_status_actyear['c_deathyear'].replace(0,np.nan)
cbdb_year_status_actyear['c_firstyear']=cbdb_year_status_actyear['c_firstyear'].replace(0,np.nan)
cbdb_year_status_actyear['c_lastyear']=cbdb_year_status_actyear['c_lastyear'].replace(0,np.nan)
cbdb_year_status_actyear['year']=cbdb_year_status_actyear['year'].replace(0,np.nan)
year=cbdb_year_status_actyear['year'].groupby(cbdb_year_status_actyear['c_personid'])
y=year.count()
num_y=y[y.isin([0])]
num_y=len(num_y)
percent_y=1-num_y/all_num_ob
percent_y=format(percent_y, '.2%')
####percent of status information
status_code=cbdb_year_status['c_status_code'].groupby(cbdb_year_status['c_personid'])
s_c=status_code.count()
num_status_c=s_c[s_c.isin([0])]
num_status_c=len(num_status_c)
percent_status_c=1-num_status_c/all_num_ob
percent_status_c=format(percent_status_c, '.2%')
####percent of father information
father=cbdb_kin_id['father_id'].groupby(cbdb_kin_id['c_personid'])
f_group=father.count()
num_father=f_group[f_group.isin([0])]
num_father=len(num_father)
percent_father=1-num_father/all_num_ob
percent_father=format(percent_father, '.2%')
####percent of mother information
mother=cbdb_kin_id['mother_id'].groupby(cbdb_kin_id['c_personid'])
m_group=mother.count()
num_mother=m_group[m_group.isin([0])]
num_mother=len(num_mother)
percent_mother=1-num_mother/all_num_ob
percent_mother=format(percent_mother, '.2%')
####percent of son information
son=cbdb_kin_id['son_id'].groupby(cbdb_kin_id['c_personid'])
s_group=son.count()
num_son=s_group[s_group.isin([0])]
num_son=len(num_son)
percent_son=1-num_son/all_num_ob
percent_son=format(percent_son, '.2%')
####percent of daughter information
daughter=cbdb_kin_id['daughter_id'].groupby(cbdb_kin_id['c_personid'])
d_group=daughter.count()
num_daughter=d_group[d_group.isin([0])]
num_daughter=len(num_daughter)
percent_daughter=1-num_daughter/all_num_ob
percent_daughter=format(percent_daughter, '.2%')

data={'CBDB':[percent_index_y,percent_birth_y,percent_death_y,percent_y,percent_status_c,percent_father,percent_mother,percent_son,percent_daughter,all_num_ob]}
cbdb_misssing_table=pd.DataFrame(data,index=['Index_year','Birth_year','Death_year','Any_year','Status','Father','Mother','Son','Daughter','Number_of_observations'])
cbdb_misssing_table.to_excel(main_path+'output/cbdb_misssing_table.xls')