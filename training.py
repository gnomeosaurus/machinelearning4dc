import setGPU
import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
import tensorflow as tf
import seaborn as sns
from pylab import rcParams
from sklearn.model_selection import train_test_split
from keras.models import Model, load_model
from keras.layers import Input, Dense, Activation
from keras.callbacks import ModelCheckpoint, TensorBoard
from keras import regularizers
from keras.layers.advanced_activations import PReLU, LeakyReLU
from sklearn.utils import shuffle
import h5py

var_names_reduced5 = ['qPFJetPt', 'qPFJetEta', 'qPFJetPhi', 'qPFJet0Pt', 'qPFJet1Pt', 'qPFJet2Pt', 'qPFJet3Pt', 'qPFJet4Pt', 'qPFJet5Pt', 'qPFJet0Eta', 'qPFJet1Eta', 'qPFJet2Eta', 'qPFJet3Eta', 'qPFJet4Eta', 'qPFJet5Eta', 'qPFJet0Phi', 'qPFJet1Phi', 'qPFJet2Phi', 'qPFJet3Phi', 'qPFJet4Phi', 'qPFJet5Phi', 'qPFJet4CHS0Pt', 'qPFJet4CHS1Pt', 'qPFJet4CHS2Pt', 'qPFJet4CHS3Pt', 'qPFJet4CHS4Pt', 'qPFJet4CHS5Pt', 'qPFJet4CHS0Eta', 'qPFJet4CHS1Eta', 'qPFJet4CHS2Eta', 'qPFJet4CHS3Eta', 'qPFJet4CHS4Eta', 'qPFJet4CHS5Eta', 'qPFJet4CHS0Phi', 'qPFJet4CHS1Phi', 'qPFJet4CHS2Phi', 'qPFJet4CHS3Phi', 'qPFJet4CHS4Phi', 'qPFJet4CHS5Phi', 'qPFJet8CHS0Pt', 'qPFJet8CHS1Pt', 'qPFJet8CHS2Pt', 'qPFJet8CHS3Pt', 'qPFJet8CHS4Pt', 'qPFJet8CHS5Pt', 'qPFJet8CHS0Eta', 'qPFJet8CHS1Eta', 'qPFJet8CHS2Eta', 'qPFJet8CHS3Eta', 'qPFJet8CHS4Eta', 'qPFJet8CHS5Eta', 'qPFJet8CHS0Phi', 'qPFJet8CHS1Phi', 'qPFJet8CHS2Phi', 'qPFJet8CHS3Phi', 'qPFJet8CHS4Phi', 'qPFJet8CHS5Phi', 'qPFJetEI0Pt', 'qPFJetEI1Pt', 'qPFJetEI2Pt', 'qPFJetEI3Pt', 'qPFJetEI4Pt', 'qPFJetEI5Pt', 'qPFJetEI0Eta', 'qPFJetEI1Eta', 'qPFJetEI2Eta', 'qPFJetEI3Eta', 'qPFJetEI4Eta', 'qPFJetEI5Eta', 'qPFJetEI0Phi', 'qPFJetEI1Phi', 'qPFJetEI2Phi', 'qPFJetEI3Phi', 'qPFJetEI4Phi', 'qPFJetEI5Phi', 'qPFJet8CHSSD0Pt', 'qPFJet8CHSSD1Pt', 'qPFJet8CHSSD2Pt', 'qPFJet8CHSSD3Pt', 'qPFJet8CHSSD4Pt', 'qPFJet8CHSSD5Pt', 'qPFJet8CHSSD0Eta', 'qPFJet8CHSSD1Eta', 'qPFJet8CHSSD2Eta', 'qPFJet8CHSSD3Eta', 'qPFJet8CHSSD4Eta', 'qPFJet8CHSSD5Eta', 'qPFJet8CHSSD0Phi', 'qPFJet8CHSSD1Phi', 'qPFJet8CHSSD2Phi', 'qPFJet8CHSSD3Phi', 'qPFJet8CHSSD4Phi', 'qPFJet8CHSSD5Phi', 'qPFJetTopCHS0Pt', 'qPFJetTopCHS1Pt', 'qPFJetTopCHS2Pt', 'qPFJetTopCHS3Pt', 'qPFJetTopCHS4Pt', 'qPFJetTopCHS5Pt', 'qPFJetTopCHS0Eta', 'qPFJetTopCHS1Eta', 'qPFJetTopCHS2Eta', 'qPFJetTopCHS3Eta', 'qPFJetTopCHS4Eta', 'qPFJetTopCHS5Eta', 'qPFJetTopCHS0Phi', 'qPFJetTopCHS1Phi', 'qPFJetTopCHS2Phi', 'qPFJetTopCHS3Phi', 'qPFJetTopCHS4Phi', 'qPFJetTopCHS5Phi', 'qCalJet0Pt', 'qCalJet1Pt', 'qCalJet2Pt', 'qCalJet3Pt', 'qCalJet4Pt', 'qCalJet5Pt', 'qCalJet0Eta', 'qCalJet1Eta', 'qCalJet2Eta', 'qCalJet3Eta', 'qCalJet4Eta', 'qCalJet5Eta', 'qCalJet0Phi', 'qCalJet1Phi', 'qCalJet2Phi', 'qCalJet3Phi', 'qCalJet4Phi', 'qCalJet5Phi', 'qCalJet0En', 'qCalJet1En', 'qCalJet2En', 'qCalJet3En', 'qCalJet4En', 'qCalJet5En', 'qPho0Pt', 'qPho1Pt', 'qPho2Pt', 'qPho3Pt', 'qPho4Pt', 'qPho5Pt', 'qPho0Eta', 'qPho1Eta', 'qPho2Eta', 'qPho3Eta', 'qPho4Eta', 'qPho5Eta', 'qPho0Phi', 'qPho1Phi', 'qPho2Phi', 'qPho3Phi', 'qPho4Phi', 'qPho5Phi', 'qPho0En', 'qPho1En', 'qPho2En', 'qPho3En', 'qPho4En', 'qPho5En', 'qgedPho0Pt', 'qgedPho1Pt', 'qgedPho2Pt', 'qgedPho3Pt', 'qgedPho4Pt', 'qgedPho5Pt', 'qgedPho0Eta', 'qgedPho1Eta', 'qgedPho2Eta', 'qgedPho3Eta', 'qgedPho4Eta', 'qgedPho5Eta', 'qgedPho0Phi', 'qgedPho1Phi', 'qgedPho2Phi', 'qgedPho3Phi', 'qgedPho4Phi', 'qgedPho5Phi', 'qgedPho0En', 'qgedPho1En', 'qgedPho2En', 'qgedPho3En', 'qgedPho4En', 'qgedPho5En', 'qMu0Pt', 'qMu1Pt', 'qMu2Pt', 'qMu3Pt', 'qMu4Pt', 'qMu5Pt', 'qMu0Eta', 'qMu1Eta', 'qMu2Eta', 'qMu3Eta', 'qMu4Eta', 'qMu5Eta', 'qMu0Phi', 'qMu1Phi', 'qMu2Phi', 'qMu3Phi', 'qMu4Phi', 'qMu5Phi', 'qMu0En', 'qMu1En', 'qMu2En', 'qMu3En', 'qMu4En', 'qMu5En', 'qMuCosm0Pt', 'qMuCosm1Pt', 'qMuCosm2Pt', 'qMuCosm3Pt', 'qMuCosm4Pt', 'qMuCosm5Pt', 'qMuCosm0Eta', 'qMuCosm1Eta', 'qMuCosm2Eta', 'qMuCosm3Eta', 'qMuCosm4Eta', 'qMuCosm5Eta', 'qMuCosm0Phi', 'qMuCosm1Phi', 'qMuCosm2Phi', 'qMuCosm3Phi', 'qMuCosm4Phi', 'qMuCosm5Phi', 'qMuCosm0En', 'qMuCosm1En', 'qMuCosm2En', 'qMuCosm3En', 'qMuCosm4En', 'qMuCosm5En', 'qMuCosmLeg0Pt', 'qMuCosmLeg1Pt', 'qMuCosmLeg2Pt', 'qMuCosmLeg3Pt', 'qMuCosmLeg4Pt', 'qMuCosmLeg5Pt', 'qMuCosmLeg0Eta', 'qMuCosmLeg1Eta', 'qMuCosmLeg2Eta', 'qMuCosmLeg3Eta', 'qMuCosmLeg4Eta', 'qMuCosmLeg5Eta', 'qMuCosmLeg0Phi', 'qMuCosmLeg1Phi', 'qMuCosmLeg2Phi', 'qMuCosmLeg3Phi', 'qMuCosmLeg4Phi', 'qMuCosmLeg5Phi', 'qMuCosmLeg0En', 'qMuCosmLeg1En', 'qMuCosmLeg2En', 'qMuCosmLeg3En', 'qMuCosmLeg4En', 'qMuCosmLeg5En', 'qPFJet4CHSPt', 'qPFJet4CHSEta', 'qPFJet4CHSPhi', 'qPFJet8CHSPt', 'qPFJet8CHSEta', 'qPFJet8CHSPhi', 'qPFJetEIPt', 'qPFJetEIEta', 'qPFJetEIPhi', 'qPFJet8CHSSDPt', 'qPFJet8CHSSDEta', 'qPFJet8CHSSDPhi', 'qPFJetTopCHSPt', 'qPFJetTopCHSEta', 'qPFJetTopCHSPhi', 'qPFChMetPt', 'qPFChMetPhi', 'qPFMetPt', 'qPFMetPhi', 'qNVtx', 'qCalJetPt', 'qCalJetEta', 'qCalJetPhi', 'qCalJetEn', 'qCalMETPt', 'qCalMETPhi', 'qCalMETEn', 'qCalMETBEPt', 'qCalMETBEPhi', 'qCalMETBEEn', 'qCalMETBEFOPt', 'qCalMETBEFOPhi', 'qCalMETBEFOEn', 'qCalMETMPt', 'qCalMETMPhi', 'qCalMETMEn', 'qSCEn', 'qSCEta', 'qSCPhi', 'qSCEtaWidth', 'qSCPhiWidth', 'qSCEnhfEM', 'qSCEtahfEM', 'qSCPhihfEM', 'qSCEn5x5', 'qSCEta5x5', 'qSCPhi5x5', 'qSCEtaWidth5x5', 'qSCPhiWidth5x5', 'qCCEn', 'qCCEta', 'qCCPhi', 'qCCEn5x5', 'qCCEta5x5', 'qCCPhi5x5', 'qPhoPt', 'qPhoEta', 'qPhoPhi', 'qPhoEn_', 'qPhoe1x5_', 'qPhoe2x5_', 'qPhoe3x3_', 'qPhoe5x5_', 'qPhomaxenxtal_', 'qPhosigmaeta_', 'qPhosigmaIeta_', 'qPhor1x5_', 'qPhor2x5_', 'qPhor9_', 'qgedPhoPt', 'qgedPhoEta', 'qgedPhoPhi', 'qgedPhoEn_', 'qgedPhoe1x5_', 'qgedPhoe2x5_', 'qgedPhoe3x3_', 'qgedPhoe5x5_', 'qgedPhomaxenxtal_', 'qgedPhosigmaeta_', 'qgedPhosigmaIeta_', 'qgedPhor1x5_', 'qgedPhor2x5_', 'qgedPhor9_', 'qMuPt', 'qMuEta', 'qMuPhi', 'qMuEn_', 'qMuCh_', 'qMuChi2_', 'qMuCosmPt', 'qMuCosmEta', 'qMuCosmPhi', 'qMuCosmEn_', 'qMuCosmCh_', 'qMuCosmChi2_', 'qMuCosmLegPt', 'qMuCosmLegEta', 'qMuCosmLegPhi', 'qMuCosmLegEn_', 'qMuCosmLegCh_', 'qMuCosmLegChi2_', 'qSigmaIEta', 'qSigmaIPhi', 'qr9', 'qHadOEm', 'qdrSumPt', 'qdrSumEt', 'qeSCOP', 'qecEn', 'qUNSigmaIEta', 'qUNSigmaIPhi', 'qUNr9', 'qUNHadOEm', 'qUNdrSumPt', 'qUNdrSumEt', 'qUNeSCOP', 'qUNecEn', 'qEBenergy', 'qEBtime', 'qEBchi2', 'qEBiEta', 'qEBiPhi', 'qEEenergy', 'qEEtime', 'qEEchi2', 'qEEix', 'qEEiy', 'qESenergy', 'qEStime', 'qESix', 'qESiy', 'qHBHEenergy', 'qHBHEtime', 'qHBHEauxe', 'qHBHEieta', 'qHBHEiphi', 'qHFenergy', 'qHFtime', 'qHFieta', 'qHFiphi', 'qPreShEn', 'qPreShEta', 'qPreShPhi', 'qPreShYEn', 'qPreShYEta', 'qPreShYPhi']

print(len(var_names_reduced5)*7+6)


sns.set(style='whitegrid', palette='muted', font_scale=1.5)

rcParams['figure.figsize'] = 14, 8

RANDOM_SEED = 42
LABELS = ["Normal", "Anomalous"]



#Choose where to load the files from
# b_h5 = '/eos/cms/store/user/fsiroky/hdf5_data/'
# b_h5       = '/eos/cms/store/user/fsiroky/lumih5/'
b_h5 = '/eos/cms/store/user/fsiroky/consistentlumih5/'
# b_h5   = '/afs/cern.ch/user/f/fsiroky/public/'
# b_h5 = '/mnt/hdf5test/'
# b_h5   = '/home/test_local/'

pds  = {1: 'BTagCSV', 2: 'BTagMu', 3: 'Charmonium', 4:'DisplacedJet', 5: 'DoubleEG',
        6: 'DoubleMuon', 7: 'DoubleMuonLowMass',
       # 8: 'FSQJets', 9: 'HighMultiplicityEOF', #NOT ENOUGH DATA, NOTEBOOK FAILES
        10: 'HTMHT', 11: 'JetHT', 12: 'MET',
       # 13: 'MinimumBias', #NOT ENOUGH DATA
        14: 'MuonEG', 15: 'MuOnia',
       # 16: 'NoBPTX',
        17: 'SingleElectron', 18: 'SingleMuon', 19: 'SinglePhoton', 20: 'Tau', 21: 'ZeroBias'
}

      
def get_jets(bg_files, bg_jets, sig_files, sig_jets):
    #Use np.empty([0,2802]) for both good and bad jets, if you use b_h5 = '/eos/cms/store/user/fsiroky/hdf5_data/'
    good_jets = np.empty([0,2813])
    bad_jets  = np.empty([0,2813])
                   # Control which time intervals files per PD to load with range in the for loop
    for i in range(0,len(bg_files)):   #0
        try:
            bg_jetfile  = h5py.File(bg_files[i],'r')
            bg_jet      = bg_jetfile[bg_jets[i]][:]
            sig_jetfile = h5py.File(sig_files[i],'r')
            sig_jet     = sig_jetfile[sig_jets[i]][:]
            # print(bad_jets.shape, bg_jet.shape)
            bad_jets    = np.concatenate((bad_jets, bg_jet), axis=0)
            good_jets = np.concatenate((good_jets, sig_jet), axis=0)
            print( "Number of good lumis: ", len(sig_jet), " Number of bad lumis: ", len(bg_jet)) 

        except OSError as error:
            print("This Primary Dataset doesn't have ", bg_jets[i], error )
            continue
    return good_jets, bad_jets


#Choose which PD to load
nbr = 11 #Jvariable

bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']

bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]

sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']

sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]

#Load good and bad jets
good_jets, bad_jets = get_jets(bg_files, bg_jets, sig_files, sig_jets)



# #Choose which PD to load
# nbr = 3 #Charmonium

# bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
#             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']

# bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
#             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]

# sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
#             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']

# sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
#             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]

# #Load good and bad jets
# good_jets2, bad_jets2 = get_jets(bg_files, bg_jets, sig_files, sig_jets)


# #Choose which PD to load
# nbr = 15 #

# bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
#             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']

# bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
#             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]

# sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
#             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']

# sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
#             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]

# #Load good and bad jets
# good_jets3, bad_jets3 = get_jets(bg_files, bg_jets, sig_files, sig_jets)



# #Choose which PD to load
# nbr = 14

# bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
#             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']

# bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
#             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]

# sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
#             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']

# sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
#             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]

# #Load good and bad jets
# good_jets4, bad_jets4 = get_jets(bg_files, bg_jets, sig_files, sig_jets)



#Assign good jets class label 0
df1 = pd.DataFrame(good_jets)
# cutted_df = df1.iloc[0:25000, :]   #Temporarily to make training faster
# df1 = cutted_df                   #Temporarily to make training faster
df1['class'] = 0

#Assign bad_jets class label  1
df2 = pd.DataFrame(bad_jets)
# cutted_df = df2.iloc[0:, :]    #Temporarily to make training faster
# df2 = cutted_df                   #Temporarily to make training faster
df2['class'] = 1

# #Assign good jets class label 0
# df3 = pd.DataFrame(good_jets2)

# df3['class'] = 0

# #Assign bad_jets class label  1
# df4 = pd.DataFrame(bad_jets2)

# df4['class'] = 1


# #Assign good jets class label 0
# df5 = pd.DataFrame(good_jets3)

# df5['class'] = 0

# #Assign bad_jets class label  1
# df6 = pd.DataFrame(bad_jets3)

# df6['class'] = 1




# df7 = pd.DataFrame(good_jets4)
# df7['class'] = 0

# df8 = pd.DataFrame(bad_jets4)
# df8['class'] = 1



# del(good_jets)
# del(bad_jets)
#Concatenate them
frames  = [df1,df2] 
#frames = [df1,df2,df3,df4,df5,df6]
# frames = [df1,df2,df3,df4,df5,df6,df7,df8]
data   = pd.concat(frames)
del(frames)
# del(df1)
# del(df2)

data.drop(2805+7, axis=1, inplace=True) #Drop per_pd flags

data = data.sort_values([2800+7,2801+7], ascending=[True,True]) #Sort by runID and then by lumiID
data = data.reset_index(drop=True)  #Reset index





# data = data.reindex(index=range(0,len(data)))
#Shuffle them randomly
# data = shuffle(data)
# data = data.reset_index(drop=True)

#Save labels and delete them from df not to cheat during training
# labels = data['class'].astype(int)
# del data['class']


import json

def json_checker(json_file, orig_runid, orig_lumid):
    outcome = 5
    for k,v in json_file.items():
        if (int(k) == orig_runid):
            for d in v: #Checks each inner loop of the json per runID
                for i in range (d[0], d[1]+1):
#                     print("key of json is ", k, " value of json is ", v)
# #                     print(v[0][0], "and", v[0][1])
#                     print("current inner list is", d, "and range is", d[0], " to ", d[1])
#                     print("i is ", i)
                    if i == orig_lumid:
#                         print("Flagging as bad")
                        outcome =0  #0 means good lumi! (to be compatible with code anomaly_detection.ipynb[mse ae])
                        return(outcome)
                
            
        
    outcome = 1 #1 means bad lumisection! (to be compatible with code anomaly_detection.ipynb [mse autoencoder])
    return(outcome)

json_file_path = '/afs/cern.ch/user/f/fsiroky/public/Cert_271036-284044_13TeV_PromptReco_Collisions16_JSON.txt'

def add_flags_from_json(output_json, data):
    output_json = json.load(open(json_file_path))
    new_json_class = np.empty([data.shape[0],1])
    for i in range(0, data.shape[0]):
        orig_runid = data[2800+7][i]
        orig_runid = int(orig_runid)
        orig_lumid = data[2801+7][i]
        orig_lumid = int(orig_lumid)
        new_json_class[i,0] = int(json_checker(output_json, orig_runid, orig_lumid))
    data['preco_json'] = new_json_class #PromptReco GOLDEN json
    return data

new_data = add_flags_from_json(json_file_path, data)
 
del(new_data)

# print("Laaalelaaa", data)

# anomalies = data[data['class'] == 1]
# normal    = data[data['class'] == 0]

# print("Number of anomalies: ", anomalies.shape)
# del(anomalies)

# print("Number of normals: ", normal.shape)
# del(normal)




runIDs  = data[2800+7].astype(int)
lumiIDs = data[2801+7].astype(int)
lumisections = data[2802+7].astype(float)

np.save('datarunIDs.npy',  runIDs)
np.save('datalumiIDs.npy', lumiIDs)
np.save('lumisections.npy', lumisections)


print("Save of RunIDs and LumiIDs done")

# print(data)
data.drop(2800+7, axis=1, inplace=True) #drop RunID before normalizing and training
data.drop(2801+7, axis=1, inplace=True) #drop LumiID before normalizing and training
print("RunID and LumiID dropped")
# print(data)

from sklearn.preprocessing import StandardScaler, MinMaxScaler, scale, RobustScaler, normalize, MaxAbsScaler

#Normalize the data to make training better
cutted_data = data.iloc[:, 0:2803+7]
#classes     = data.iloc[:, 2805:2806] 
classes      = data.iloc[:,-1] #Take PromptReco json


# print(classes.shape)
np_scaled = StandardScaler().fit_transform(cutted_data.values)
# np_scaled = MaxAbsScaler().fit_transform(np_scaled) 

# print("1111",np_scaled)


# np_scaled = scale(cutted_data, axis = 1, with_mean=True, with_std=True, copy=True)
datas = pd.DataFrame(np_scaled)
# datas = pd.DataFrame(np_scaled, index=cutted_data.index, columns=cutted_data.columns)

# print("2222",datas)

# del(np_scaled)
del(cutted_data)
# print("Datas first: ", datas)
datas[2803+7] = runIDs   #Append runID back after scaling
datas[2804+7] = lumiIDs  #Append lumiID back after scaling
datas['qlabel'] = classes  #qlabel is goldenJSON now

# print("After scale", datas)


# X_train, X_test = train_test_split(datas, test_size=0.15, random_state=RANDOM_SEED) # This works when we split rndmly
split_nbr = round(datas.shape[0]*0.20)  #0.10 means 10% to the validation set

print(datas.shape)
X_train = datas.iloc[0:(datas.shape[0] - split_nbr) ,:]
X_test  = datas.iloc[(datas.shape[0] - split_nbr): (datas.shape[0]) ,:]
last_train_idx = X_train.shape[0]

np.save('last_train_idx.npy', last_train_idx)
# print(X_train.shape)
# print(X_test.shape)

del(datas)
X_train = X_train[X_train['qlabel']== 0]
# print(X_train)
X_train = X_train.drop(['qlabel'], axis=1)

ae_lumis = X_train[2800+7].astype(float)
# print("ae lumis", ae_lumis, "ae_lumis shape", ae_lumis.shape)
# print("XTEEEEST before PerPD json beginn")
# print(X_test)


json_file_path_PD = '/afs/cern.ch/user/f/fsiroky/Documents/gen_config/jsons/JetHT.json'

def add_flags_from_json_PD(output_json, X_test):
    output_json = json.load(open(json_file_path))
    new_json_class = np.empty([X_test.shape[0],1])
    for i in range(0, X_test.shape[0]):
        orig_runid = X_test[2803+7][i+last_train_idx]
        # orig_runid = int(orig_runid)
        orig_lumid = X_test[2804+7][i+last_train_idx]
        # orig_lumid = int(orig_lumid)
        new_json_class[i,0] = int(json_checker(output_json, orig_runid, orig_lumid))
    X_test['PD_json'] = new_json_class
    return X_test

new_data = add_flags_from_json_PD(json_file_path_PD, X_test)

del(new_data)
# print("Now new X_test label")
# print(X_test)




#y_test = X_test['qlabel']

y_test = X_test['PD_json']

print("Number of good lumis in X_test: ", len(X_test[y_test==0]))
print("Number of bad lumis in X_test: ",  len(X_test[y_test==1]))

X_test.drop(['qlabel'], axis=1, inplace=True)
X_test.drop(['PD_json'], axis=1, inplace=True)


X_train.drop(2803+7, axis=1, inplace=True) #drop RunID before training
X_train.drop(2804+7, axis=1, inplace=True) #drop LumiID before training
X_test.drop(2803+7, axis=1, inplace=True) #drop RunID before training
X_test.drop(2804+7, axis=1, inplace=True) #drop LumiID before training


# print("X_test before saving: ", X_test)

luminosity_vals = lumisections.iloc[:int(last_train_idx)].values

X_train = X_train.values
X_test = X_test.values

np.save('X_testfor3pds_model.npy', X_test)
np.save('y_testfor3pds_model.npy', y_test)

from keras.layers import concatenate

from keras.utils.generic_utils import get_custom_objects

# def custom_activation(x):
#     return ((((x**2+1)**(.5) - 1) / 2 ) + x)

# get_custom_objects().update({'custom_activation': custom_activation})

input_dim = X_train.shape[1]
encoding_dim = 2000


input_layer = Input(shape=(input_dim, ))

# prellll = PReLU(alpha_initializer='zeros', alpha_regularizer=None, alpha_constraint=None, shared_axes=None)

# prellll = LeakyReLU(alpha=0.3)
# encoder = Dense(2600, #activation="custom_activation",
# # kernel_regularizer=regularizers.l2(0.005),
#                 activity_regularizer=regularizers.l1(10e-5) 
#                               )(input_layer)
# encoder = prellll(encoder)

# encoder = prellll(encoder)
# luminosity_neuron = Input(shape=(1,))

# luminosity_neuron_dense = Dense(1,)(luminosity_neuron)

# prellll = LeakyReLU(alpha=0.3)
# encoded = Dense(2200, #activation="relu", 
# # kernel_regularizer=regularizers.l2(0.005),
#                 # activity_regularizer=regularizers.l1(10e-5)    
#                 )(encoder)
# encoded = prellll(encoded)


# encoded = Dense(2600, activation='relu')(encoder)

# x = concatenate([encoded, luminosity_neuron_dense])
# prellll = PReLU(alpha_initializer='zeros', alpha_regularizer=None, alpha_constraint=None, shared_axes=None)
prellll = LeakyReLU(alpha=0.2)
encoded = Dense(encoding_dim, #activation="relu", 
# kernel_regularizer=regularizers.l2(10e-5),
                activity_regularizer=regularizers.l1(10e-5) 
                   )(input_layer)
encoded = prellll(encoded)

# luminosity_neuron = Input(shape=(1,), name='l_neu')
# decoded = Dense(2600, activation='relu')(encoded)

# x = concatenate([decoded, luminosity_neuron])

# prellll = LeakyReLU(alpha=0.3)
# decoded = Dense(2200, # activation='relu',
#     # activity_regularizer=regularizers.l1(10e-5)
# )(encoded)
# decoded = prellll(decoded)


# prellll = PReLU(alpha_initializer='zeros', alpha_regularizer=None, alpha_constraint=None, shared_axes=None)

# prellll = LeakyReLU(alpha=0.3)
# decoded = Dense(2600, # activation='relu',
#     # activity_regularizer=regularizers.l1(10e-5)
# )(encoded)
# decoded = prellll(decoded)

# encoder = Dense(int(encoding_dim / 1.2), activation="relu")(encoder)

# encoder = Dense(int(encoding_dim / 1.5), activation="relu")(encoder)

# decoder = Dense(2000, activation='relu')(encoded)
# prellll = PReLU(alpha_initializer='zeros', alpha_regularizer=None, alpha_constraint=None, shared_axes=None)
prellll = LeakyReLU(alpha=0.2)

decoder = Dense(input_dim)(encoded)
decoder = prellll(decoder)
# decoder = Dense(input_dim)(encoded)

autoencoder = Model(inputs=input_layer, outputs=decoder)


nb_epoch = 100
batch_size = 256
from keras.optimizers import Adam
# adam = Adam(lr=0.001, beta_1=0.9, beta_2=0.999, epsilon=None, decay=0.0)

autoencoder.compile(optimizer='Adam', 
                    loss='mean_squared_error',
                    # metrics=['accuracy']
                   )

checkpointer = ModelCheckpoint(filepath="dec_model.h5",
                               verbose=0,
                               save_best_only=True)

# tensorboard = TensorBoard(log_dir='./logs',
#                           histogram_freq=0,
#                           write_graph=True,
#                           write_images=True)

history = autoencoder.fit(X_train, X_train,
                    epochs=nb_epoch,
                    batch_size=batch_size,
                    shuffle=True,
                    validation_data=(X_test, X_test),
                    verbose=2,
                    callbacks=[checkpointer
                              #,tensorboard
                              ]).history

