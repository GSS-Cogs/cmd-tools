import sys

from cmdtools.loader import CmdLoader

def cmdload(dataset_id, v4):
    """
    Wrapper for calling loader.CmdLoader from the command line
    """
    cmd_loader = CmdLoader(dataset_id, v4)
    cmd_loader.upload_data_to_florence()

if __name__ == "__main__":
    
    dataset_id = sys.argv[1],
    v4 = sys.argv[2]

    print(f'''Running CmdLoader with arguments:
    
    dataset id: {dataset_id}
    path to v4: {v4}
    ''')
    
    cmdload(dataset_id, v4)
