#!/usr/bin/env python3


def RDataFrame(*args, **kwargs):
    """
    Create an RDataFrame object that can run computations on an OSCAR cluster.
    """

    from DistRDF.Backends.OSCAR import Backend
    oscarclient = kwargs.get("oscarclient", None)
    oscarbackend = Backend.OSCARBackend(oscarclient=oscarclient)

    return oscarbackend.make_dataframe(*args, **kwargs)
