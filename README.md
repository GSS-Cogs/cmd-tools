# cmd-tools

Various utilities for working with CMD flavoured data transformations.

### Install

```sh
pip install --no-cache git+git://github.com/GSS-Cogs/cmdtools.git#egg=cmdtools
```

### Environment Variables

The upload tool from cmdtools uses the following envionrment variables.

|          Variable         |              Description           |     Default   |
|---------------------------|------------------------------------|---------------|
| FLORENCE_USERNAME         | A valid florence username          | NONE, load will fail  |
| FLORENCE_PASSWORD         | The password for this username     | NONE, load will fail  |
| CMD_DATASET_UPLOAD_BUCKET | The bucket we're uplaoding v4 files to | https://s3-eu-west-1.amazonaws.com/ons-dp-develop-publishing-uploaded-datasets |
| CMD_API_ROOT | The api root for the environment in question | https://publishing.develop.onsdigital.co.uk |

To ser envionment variables export them via, eg `export FLORENCE_USERNAME=<username>`.

To set them permenently add these export lines to either `~/.zshrc` or `~/.bashrc` (whichever youre using, just do eg `nano ~/.bashrc`) then restart your terminal.

### CMD Uploader

To use within python use:

```python
from cmdtools.loader import CmdLoader
cmdloader = CmdLoader(<dataset_id>, <path_to_v4>)
cmdloader.upload_data_to_florence()
```

Once installed, the cmd loader can be run from the command line via:

```sh
# TODO
```
