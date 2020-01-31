# coding: utf-8

from parsr_client import ParserClient
import json
import os


def Convert(current_path):
    
    RootDir = current_path + 'resource/pubs'
    pdf_Dir = RootDir + '/pdf'
    config_path = current_path + 'bin/sampleConfig.json'
    for lists in os.listdir(pdf_Dir): 
        pdf_path = pdf_Dir + '/' + lists
        # Send Document for processing 
        job = parsr.send_document(
            file = pdf_path,
            config = config_path,
            wait_till_finished=True,
            save_request_id=True,
        ) 
        
        # Get the full JSON output. 
        json_Dir = RootDir + '/json'
        mkdir(json_Dir)
        json_path = json_Dir + '/' + lists + '.json'
        with open(json_path, 'w', encoding="utf-8") as outfile:
            json.dump(parsr.get_json(), outfile, indent=2, ensure_ascii=False)
            
        # Get the raw text output.    
        text_Dir = RootDir  + '/text'
        mkdir(text_Dir)
        text_path =  text_Dir + '/' + lists + '.txt'
        with open(text_path, 'w', encoding="utf-8") as outfile:
            outfile.write(parsr.get_text())


def mkdir(path):
    # Check if the folder is already exist, if not, create a new one.
    folder = os.path.exists(path)
 
    if not folder:                   
        os.makedirs(path)

         
        
if __name__ == "__main__":
    
    # Initialize the client object.
    parsr = ParserClient('localhost:3001')
    
    current_path = os.path.dirname(os.path.dirname(__file__))
    
    Convert(current_path)





