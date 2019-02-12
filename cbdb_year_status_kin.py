# -*- coding: utf-8 -*-
"""
Created on Fri Jan  4 11:46:46 2019

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



zzz_biog_main=pd.read_excel(main_path+'input/ZZZ_BIOG_MAIN.xlsx')
zzz_kin_biog_addr=pd.read_excel(main_path+'input/ZZZ_KIN_BIOG_ADDR.xlsx')
zzz_status=pd.read_excel(main_path+'input/ZZZ_STATUS_DATA.xlsx')
zzz_posted_to_addr_data=pd.read_excel(main_path+'input/ZZZ_POSTED_TO_ADDR_DATA.xlsx')
zzz_biog_source_data=pd.read_excel(main_path+'input/ZZZ_BIOG_SOURCE_DATA.xlsx')


name=zzz_biog_main.loc[:,['c_personid','c_name','c_name_chn']]
status=zzz_status.loc[:,['c_personid','c_status_code','c_status_desc','c_status_desc_chn']]
biog_main=zzz_biog_main.loc[:,['c_personid','c_name','c_name_chn','c_female','c_index_year','c_birthyear','c_deathyear','c_death_age']]
active_year=zzz_posted_to_addr_data.loc[:,['c_personid','c_firstyear','c_lastyear']]
kin=zzz_kin_biog_addr.loc[:,['c_personid','c_person_name','c_person_name_chn','c_link_chn','c_link_desc','c_node_id','c_node_name','c_node_chn']]
#x=kin[kin['c_personid']==40589]
#xx=kin[kin['c_link_desc']=='D©']
cbdb_year_status=pd.merge(biog_main,status,on='c_personid',how='left')
cbdb_year_status.to_csv(main_path+'output/cbdb_year_status.csv', index=False)

######get the minimum of the C_firstyear and c_lastyear of each person. Then we can not say the minimum of lastyear must be larger then the minimum of firstyear
active_year=active_year.replace(0,np.nan)
a_year_f_g=active_year['c_firstyear'].groupby(active_year['c_personid'])
a_y_f_g=a_year_f_g.min()
a_y_f_g=a_y_f_g.reset_index()


a_year_l_g=active_year['c_lastyear'].groupby(active_year['c_personid'])
a_y_l_g=a_year_l_g.min()
a_y_l_g=a_y_l_g.reset_index()


active_year_min=pd.merge(a_y_f_g,a_y_l_g,on='c_personid',how='left')
active_year_min.rename(columns={'c_firstyear':'min_firstyear','c_lastyear':'min_lastyear'},inplace=True)
cbdb_year_status=pd.read_csv(main_path+'output/cbdb_year_status.csv')

cbdb_year_status_actyear=pd.merge(cbdb_year_status,active_year_min,on='c_personid',how='left')
cbdb_year_status_actyear=cbdb_year_status_actyear.loc[:,['c_personid','c_name','c_name_chn','c_female','c_index_year','c_birthyear','c_deathyear','c_death_age','min_firstyear','min_lastyear','c_status_code','c_status_desc','status_desc_ch']]
cbdb_year_status_actyear=cbdb_year_status_actyear.replace(0,np.nan)


cbdb_year_status_actyear.to_csv(main_path+'output/cbdb_year_status_actyear.csv',index=False)

#cbdb_year_status_actyear=pd.read_csv(main_path+'output/cbdb_year_status_actyear.csv')

#kin relationship description
kin_relationship=kin.loc[:,['c_link_chn','c_link_desc']]
kin_relationship.drop_duplicates(inplace=True)
#Father/Mother and Son/Daughter
relative_father=['F','F*','F^','F°','F!','F#']
#F*:the one who adopts you as his heir.F^:stepfather. F°:adopting father.F!: the barstard's father. F#:Mother's husband not biologically related to the subject
relative_mother=['M','M^','M!','M~','M°','M(C)','M+','M#','M*']
#M^:stepmother. M!: the barstard's mother. M*:the one who adopts you as the heir of her husband. M~:legitimate wife as nominal mother to children of concubine.
#M°:adopting mother.M(C): father's concubine. M+:father's former wife . M#:Father's wife  not biologically related to the subject
n=[str(n) for n in range(1,31)]
daughter_n=['D'+i for i in n]
daughter=['D','D (only daughter)','D©']+daughter_n#D©:daughter of concubine
relative_daughter=['D°','D^','D#']+daughter
#D°:adopted daughter.D^:step-daughter.D#:Daughter of spouse not born to the subject
n=[str(n) for n in range(1,31)]
son_n=['S'+i for i in n]
son=son_n+['S','Sn','S (only son)','S (only surviving son)','CS','S (eldest surviving son)','S!','S©']#Sn:the youngest son. CS:concubine's child . S!:Bastard. S©:Son of concubine
relative_son=['S*','S^','S°']+son
#S^:stepson.S°:adopted son. S*:Adopted heir. S#:Son of spouse not born to the subject.

#Father_data
father_data=kin[kin.c_link_desc.isin(['F','F!'])]
father_data=father_data.loc[:,['c_personid','c_node_id','c_node_name','c_node_chn']]
father_data.rename(columns={'c_node_id':'father_id','c_node_name':'father_name','c_node_chn':'father_chn'},inplace=True)

#Mother_data
mother_data=kin[kin.c_link_desc.isin(['M','M!'])]
mother_data=mother_data.loc[:,['c_personid','c_node_id','c_node_name','c_node_chn']]
mother_data.rename(columns={'c_node_id':'mother_id','c_node_name':'mother_name','c_node_chn':'mother_chn'},inplace=True)

#son_data
son_data=kin[kin.c_link_desc.isin(son)]
son_data=son_data.loc[:,['c_personid','c_node_id','c_node_name','c_node_chn']]
son_data.rename(columns={'c_node_id':'son_id','c_node_name':'son_name','c_node_chn':'son_chn'},inplace=True)

#daughter_data
daughter_data=kin[kin.c_link_desc.isin(daughter)]
daughter_data=daughter_data.loc[:,['c_personid','c_node_id','c_node_name','c_node_chn']]
daughter_data.rename(columns={'c_node_id':'daughter_id','c_node_name':'daughter_name','c_node_chn':'daughter_chn'},inplace=True)

cbdb_kin_f=pd.merge(name,father_data,on='c_personid',how='left')
c_kin_fg=cbdb_kin_f['father_id'].groupby(cbdb_kin_f['c_personid'])
c_k_fg=c_kin_fg.count()
c_k_fg=c_k_fg.reset_index()
more_than_one_f_id=c_k_fg[c_k_fg['father_id']>=2]# the list of id of those who have more than 1 father in CBDB data
more_than_one_f_id.to_excel(main_path+'output/more_than_one_f_id.xls', index=False)


cbdb_kin_m=pd.merge(name,mother_data,on='c_personid',how='left')
c_kin_mg=cbdb_kin_m['mother_id'].groupby(cbdb_kin_m['c_personid'])
c_k_mg=c_kin_mg.count()
c_k_mg=c_k_mg.reset_index()
more_than_one_m_id=c_k_mg[c_k_mg['mother_id']>=2]# the list of id of those who have more than 1 mother in CBDB data
more_than_one_m_id.to_excel(main_path+'output/more_than_one_m_id.xls', index=False)

cbdb_kin=pd.merge(name,father_data,on='c_personid',how='left')
cbdb_kin=pd.merge(cbdb_kin,mother_data,on='c_personid',how='left')
cbdb_kin=pd.merge(cbdb_kin,son_data,on='c_personid',how='left')
cbdb_kin=pd.merge(cbdb_kin,daughter_data,on='c_personid',how='left')
cbdb_kin.to_csv(main_path+'output/cbdb_kin.csv', index=False)
cbdb_kin_id=cbdb_kin.loc[:,['c_personid','father_id','mother_id','son_id','daughter_id']]
cbdb_kin_id.to_csv(main_path+'output/cbdb_kin_id.csv', index=False)

#####Source data
source=zzz_biog_source_data.loc[:,['c_personid','c_textid']]
cbdb_source=pd.merge(name,source,on='c_personid',how='left')
cbdb_source_g=cbdb_source['c_textid'].groupby(cbdb_source['c_personid'])
c_s_g=cbdb_source_g.count()
c_s_g=c_s_g.reset_index()
c_s_g.to_csv(main_path+'output/cbdb_source.csv',index=False)
