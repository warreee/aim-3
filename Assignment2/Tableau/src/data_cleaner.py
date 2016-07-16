import graphlab as gl
import graphlab.aggregate as agg
data_in_path = "/home/warreee/projects/2016-SS-Assignments/Assignment2/Tableau/raw_data/"

data_out_path = "/home/warreee/projects/2016-SS-Assignments/Assignment2/Tableau/clean_data/"


agora = gl.SFrame.read_csv(data_in_path + "agora1.txt", delimiter=' ', header=False)
agora['X2'] = agora['X2'].apply(lambda x : x.replace(',', ''))
agora['X3'] = agora['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
agora = agora.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
agora.save(data_out_path + "agora.csv", format='csv')

biomedisch = gl.SFrame.read_csv(data_in_path + "biomedisch1.txt", delimiter=' ', header=False)
biomedisch['X2'] = biomedisch['X2'].apply(lambda x : x.replace(',', ''))
biomedisch['X3'] = biomedisch['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
biomedisch = biomedisch.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
biomedisch.save(data_out_path + "biomedisch.csv", format='csv')

cba = gl.SFrame.read_csv(data_in_path + "cba1.txt", delimiter=' ', header=False)
cba['X2'] = cba['X2'].apply(lambda x : x.replace(',', ''))
cba['X3'] = cba['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
cba = cba.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
cba.save(data_out_path + "cba.csv", format='csv')

centrale = gl.SFrame.read_csv(data_in_path + "centrale1.txt", delimiter=' ', header=False)
centrale['X2'] = centrale['X2'].apply(lambda x : x.replace(',', ''))
centrale['X3'] = centrale['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
centrale = centrale.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
centrale.save(data_out_path + "centrale.csv", format='csv')

economie = gl.SFrame.read_csv(data_in_path + "economie1.txt", delimiter=' ', header=False)
economie['X2'] = economie['X2'].apply(lambda x : x.replace(',', ''))
economie['X3'] = economie['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
economie = economie.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
economie.save(data_out_path + "economie.csv", format='csv')

kulak = gl.SFrame.read_csv(data_in_path + "kulak1.txt", delimiter=' ', header=False)
kulak['X2'] = kulak['X2'].apply(lambda x : x.replace(',', ''))
kulak['X3'] = kulak['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
kulak = kulak.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
kulak.save(data_out_path + "kulak.csv", format='csv')

letteren = gl.SFrame.read_csv(data_in_path + "letteren1.txt", delimiter=' ', header=False)
letteren['X2'] = letteren['X2'].apply(lambda x : x.replace(',', ''))
letteren['X3'] = letteren['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
letteren = letteren.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
letteren.save(data_out_path + "letteren.csv", format='csv')

psychologie = gl.SFrame.read_csv(data_in_path + "psychologie1.txt", delimiter=' ', header=False)
psychologie['X2'] = psychologie['X2'].apply(lambda x : x.replace(',', ''))
psychologie['X3'] = psychologie['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
psychologie = psychologie.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
psychologie.save(data_out_path + "psychologie.csv", format='csv')

rechten = gl.SFrame.read_csv(data_in_path + "rechten1.txt", delimiter=' ', header=False)
rechten['X2'] = rechten['X2'].apply(lambda x : x.replace(',', ''))
rechten['X3'] = rechten['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
rechten = rechten.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
rechten.save(data_out_path + "rechten.csv", format='csv')

socialewet = gl.SFrame.read_csv(data_in_path + "socialewet1.txt", delimiter=' ', header=False)
socialewet['X2'] = socialewet['X2'].apply(lambda x : x.replace(',', ''))
socialewet['X3'] = socialewet['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
socialewet = socialewet.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
socialewet.save(data_out_path + "socialewet.csv", format='csv')

wijsbegeerte = gl.SFrame.read_csv(data_in_path + "wijsbegeerte1.txt", delimiter=' ', header=False)
wijsbegeerte['X2'] = wijsbegeerte['X2'].apply(lambda x : x.replace(',', ''))
wijsbegeerte['X3'] = wijsbegeerte['X3'].apply(lambda x : ':'.join(x.split(':')[0:2]))
wijsbegeerte = wijsbegeerte.groupby(key_columns=['X3', 'X2'], operations={'average': agg.MEAN('X1')}).sort(sort_columns=['X2', 'X3'])
wijsbegeerte.save(data_out_path + "wijsbegeerte.csv", format='csv')