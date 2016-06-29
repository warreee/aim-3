import graphlab as gl

datapath = "/home/warreee/projects/2016-SS-Assignments/Assignment2/Tableau/raw_data/"


agora = gl.SFrame.read_csv(datapath + "agora1.txt", delimiter=' ', header=False)
agora['X2'] = agora['X2'].apply(lambda x : x.replace(',', ''))
agora['X3'] = agora['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

biomedisch = gl.SFrame.read_csv(datapath + "biomedisch1.txt", delimiter=' ', header=False)
biomedisch['X2'] = biomedisch['X2'].apply(lambda x : x.replace(',', ''))
biomedisch['X3'] = biomedisch['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

cba = gl.SFrame.read_csv(datapath + "cba1.txt", delimiter=' ', header=False)
cba['X2'] = cba['X2'].apply(lambda x : x.replace(',', ''))
cba['X3'] = cba['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

centrale = gl.SFrame.read_csv(datapath + "centrale1.txt", delimiter=' ', header=False)
centrale['X2'] = centrale['X2'].apply(lambda x : x.replace(',', ''))
centrale['X3'] = centrale['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

economie = gl.SFrame.read_csv(datapath + "economie1.txt", delimiter=' ', header=False)
economie['X2'] = economie['X2'].apply(lambda x : x.replace(',', ''))
economie['X3'] = economie['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

kulak = gl.SFrame.read_csv(datapath + "kulak1.txt", delimiter=' ', header=False)
kulak['X2'] = kulak['X2'].apply(lambda x : x.replace(',', ''))
kulak['X3'] = kulak['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

letteren = gl.SFrame.read_csv(datapath + "letteren1.txt", delimiter=' ', header=False)
letteren['X2'] = letteren['X2'].apply(lambda x : x.replace(',', ''))
letteren['X3'] = letteren['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

psychologie = gl.SFrame.read_csv(datapath + "psychologie1.txt", delimiter=' ', header=False)
psychologie['X2'] = psychologie['X2'].apply(lambda x : x.replace(',', ''))
psychologie['X3'] = psychologie['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

rechten = gl.SFrame.read_csv(datapath + "rechten1.txt", delimiter=' ', header=False)
rechten['X2'] = rechten['X2'].apply(lambda x : x.replace(',', ''))
rechten['X3'] = rechten['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

socialewet = gl.SFrame.read_csv(datapath + "socialewet1.txt", delimiter=' ', header=False)
socialewet['X2'] = socialewet['X2'].apply(lambda x : x.replace(',', ''))
socialewet['X3'] = socialewet['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))

wijsbegeerte = gl.SFrame.read_csv(datapath + "wijsbegeerte1.txt", delimiter=' ', header=False)
wijsbegeerte['X2'] = wijsbegeerte['X2'].apply(lambda x : x.replace(',', ''))
wijsbegeerte['X3'] = wijsbegeerte['X3'].apply(lambda x : ':'.join(x.split(':')[0:3]))