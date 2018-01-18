Q_all = 'L1tmu:GOOD,Hlt:GOOD,Lumi:GOOD,All:GOOD'

Q_1   = ',Pix:GOOD,Strip:GOOD,Track:GOOD'
Q_2   = ',Ecal:GOOD,Es:GOOD,Egamma:GOOD'
Q_3   = ',Hcal:GOOD,Jetmet:GOOD'
Q_4   = ',Dt:GOOD,Rpc:GOOD,Csc:GOOD,Muon:GOOD'
Q_not_mu  = ',L1tcalo:GOOD' #Virginia's feedback:All except: BTagCSV, MET, Tau, BTagMu, Charmonium, DoubleMuon, SingleMuon, DoubleMuonLowMass, MuOnia, MuonEG

d_1   = 'Bpix,Fpix,Tibtid,TecM,TecP,Tob'
d_2   = ',Ebm,Ebp,EeM,EeP,EsM,EsP'
photon_d2 = 'Ebm,Ebp,EeM,EeP,EsM,EsP' #get rid of the comma for photons
d_3   = ',HbheA,HbheB,HbheC,Hf,Ho'
d_4   = ',Dtm,Dtp,Dt0,CscM,CscP,Rpc'

# mapping = {Q_1 : d_1,
#            Q_2 : d_2,
#            Q_3 : d_3,
#            Q_4 : d_4}

my_dict  = {"BTagCSV"         :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3, Q_4],
                                  "DCS"   : [       d_1, d_2, d_3, d_4]},
           "BTagMu"           :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3, Q_4],
                                  "DCS"   : [       d_1, d_2, d_3, d_4]},   
           "Charmonium"       :  {"QFLAGS": [Q_all, Q_1,           Q_4],
                                  "DCS"   : [       d_1,           d_4]},
           "DisplacedJet"     :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3     , Q_not_mu],
                                  "DCS"   : [       d_1, d_2, d_3     ]},
           "DoubleEG"         :  {"QFLAGS": [Q_all, Q_1, Q_2          , Q_not_mu],
                                  "DCS"   : [       d_1, d_2          ]},
           "DoubleMuon"       :  {"QFLAGS": [Q_all, Q_1,           Q_4],
                                  "DCS"   : [       d_1,           d_4]},
           "DoubleMuonLowMass":  {"QFLAGS": [Q_all, Q_1,           Q_4],
                                  "DCS"   : [       d_1,           d_4]},
           "HTMHT"            :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3     , Q_not_mu],
                                  "DCS"   : [       d_1, d_2, d_3     ]},
           "JetHT"            :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3     , Q_not_mu],
                                  "DCS"   : [       d_1, d_2, d_3     ]},                                             
           "MET"              :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3, Q_4],
                                  "DCS"   : [       d_1, d_2, d_3, d_4]},
           "MuonEG"           :  {"QFLAGS": [Q_all, Q_1, Q_2,      Q_4],
                                  "DCS"   : [       d_1, d_2,      d_4]},
           "MuOnia"           :  {"QFLAGS": [Q_all, Q_1,           Q_4],
                                  "DCS"   : [       d_1,           d_4]},
           "SingleElectron"   :  {"QFLAGS": [Q_all, Q_1, Q_2          , Q_not_mu],
                                  "DCS"   : [       d_1, d_2          ]},
           "SingleMuon"       :  {"QFLAGS": [Q_all, Q_1,           Q_4],
                                  "DCS"   : [       d_1,           d_4]},
           "SinglePhoton"     :  {"QFLAGS": [Q_all,      Q_2          , Q_not_mu],
                                  "DCS"   : [            photon_d2    ]},
           "Tau"              :  {"QFLAGS": [Q_all, Q_1, Q_2, Q_3, Q_4],
                                  "DCS"   : [       d_1, d_2, d_3, d_4]},
           "ZeroBias"         :  {"QFLAGS": [Q_all, Q_1               , Q_not_mu],
                                  "DCS"   : [       d_1               ]}
          }

          	             
import configparser

base = '2016_config_'
form = '.cfg'
for pd, vals in my_dict.items():
  end_path = base + str(pd) + form 
  config = configparser.RawConfigParser()
  config.optionxform = str
  config['Common'] = {'Runreg'   : 'http://runregistry.web.cern.ch/runregistry/',
                    'Dataset'    : '/PromptReco/%Collisions2016%:275657-284044',    
                    'Group'      : 'Collisions16',                                  
                    'RunMin'     : '275657',
                    'RunMax'     : '284044',
                    'BField_thr' : '3.7',
                    'BEAMPRESENT': 'True',
                    'BEAMSTABLE' : 'True',
                    'INJECTION'  : '25ns%',
                    'Beam_Ene'   : '6500',
                    'DSSTATE'    : 'COMPLETED',
                    'NOLOWPU'    : 'True'}
  for k1, v1 in vals.items():
    if k1   == 'QFLAGS':
      # config[str(pd)] = {}
      config['Common']['QFLAGS']   = ''.join((v1))
    elif k1 == 'DCS':
      config['Common']['DCS']      = ''.join((v1))   
      config['Common']['JSONFILE'] =  '/afs/cern.ch/user/f/fsiroky/Documents/gen_config/jsons/'+str(pd)+'.json'    
  with open(end_path, 'w') as configfile:
    config.write(configfile)