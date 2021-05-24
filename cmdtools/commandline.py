import sys

from cmdtools.loader import CmdLoader

def cmdload():
    """
    Wrapper for calling loader.CmdLoader from the command line
    """

    # TODO - should probably use an argparser

    credentials = sys.argv[1],
    dataset_id = sys.argv[2],
    v4 = sys.argv[3]

    print(f'''Running CmdLoader with arguments:
    
    credentials: {credentials}
    dataset id: {dataset_id}
    path to v4: {v4}
    ''')

    cmd_loader = CmdLoader(credentials, dataset_id, v4)
    cmd_loader.upload_data_to_florence()
