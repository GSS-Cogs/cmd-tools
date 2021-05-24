# Cmd-utils

Various utilities for working with CMD flavoured dara transformations.


### CMD Uploader

Usage is:

```python
from cmdutils import CmdApi
cmdapi = CmdApi(<path_to_credentials>, <dataset_id>, <path_to_v4>)
cmdapi.upload_data_to_florence()
```