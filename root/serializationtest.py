import ROOT
from dask.distributed import Client, LocalCluster


#cluster = LocalCluster(n_workers=4, threads_per_worker=1, processes=True)
#client = Client(threads_per_worker=1, processes=True)
client = Client("172.21.0.2:8786")
RDataFrame = ROOT.RDF.Experimental.Distributed.Dask.RDataFrame
RunGraphs = ROOT.RDF.Experimental.Distributed.RunGraphs

import time
start = time.time()

df = RDataFrame(1000, daskclient=client)

# Set the random seed and define two columns of the dataset with random numbers.
ROOT.gRandom.SetSeed(1)
df_1 = df.Define("gaus", "gRandom->Gaus(10, 1)").Define("exponential", "gRandom->Exp(10)")

# Book an histogram for each column
h_gaus = df_1.Histo1D(("gaus", "Normal distribution", 50, 0, 30), "gaus")
h_exp = df_1.Histo1D(("exponential", "Exponential distribution", 50, 0, 30), "exponential")

# Plot the histograms side by side on a canvas
c = ROOT.TCanvas("distrdf002", "distrdf002", 800, 400)
c.Divide(2, 1)
c.cd(1)
h_gaus.DrawCopy()
c.cd(2)
h_exp.DrawCopy()

# Save the canvas
end = time.time()
print(end-start)
