# RegriddERA5_CAMFV

**Note** this script aint slow, but it aint fast... 

This script quickly gathers the ML (model level) era5 data from the RDA at NCAR. 
This Script only works on the NCAR machines ... sorry. 

It's ideal use case is for building nudging files... 

To run:
- edit GatherERA5_distributed where it says "modify"
- edit submit_GatherERA5.sh -- to include your project # etc. etc.

```
    qsub submit_GatherERA5.sh
```

IF you'd rather just use a jupyter notebook on jupyterhub.. see "JupyterExample.ipynb"