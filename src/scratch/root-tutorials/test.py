import ROOT
import numpy as np

x = np.array([1,2,3], dtype=np.int32)
y = np.array([4,5,6], dtype=np.float64)

df = ROOT.RDF.MakeNumpyDataFrame({'x':x , 'y':y})

df = df.Define('z', 'x+y')

df.Display().Print()

df.Snapshot('tree', 'df032_MakeNumpyDataFrame.root')
