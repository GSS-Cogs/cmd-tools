import datetime
import json
import os
import requests
from requests import RequestException

class CmdApi:

    def __init__(self, credentials, dataset_id, v4, api_root='https://publishing.develop.onsdigital.co.uk'):

        # Note: using a kwarg so we can change it whenever without rewriting anything
        self.api_root = api_root

        # Set endpoints we're going to need
        self.zebedee_url = f'{self.api_root}/zebedee/login'
        self.recipe_api_url = f'{self.api_root}/recipes'
        self.upload_url = f'{self.api_root}/upload'
        self.dataset_instances_api_url = f'{self.api_root}/dataset/instances'
        self.dataset_jobs_api_url = f'{self.api_root}/dataset/jobs'
        self.base_s3_url = 'https://s3-eu-west-1.amazonaws.com/ons-dp-develop-publishing-uploaded-datasets'

        self.credentials = credentials
        self.dataset_id = dataset_id
        self.v4 = v4

        self.recipe_api_contents = None  # for caching, ie only get this endpoint once
        
        self.recipe_api_limit = 1000
        self.dataset_jobs_api_limit = 1000

    # Note: We're wrapping requests.get so if we want to add clever things like retries and
    # exponential backoff, we only need to add it in one place.
    def _get(self, url, **kwargs):
        """Get things with requests"""
        return requests.get(url, kwargs)


    # Note: We're wrapping requests.post so if we want to add clever things like retries and
    # exponential backoff, we only need to add it in one place.
    def _post(self, url, **kwargs):
        """Get things with requests"""
        return requests.post(url, kwargs)
        
    # Note: We're wrapping requests.put so if we want to add clever things like retries and
    # exponential backoff, we only need to add it in one place.
    def _put(self, url, **kwargs):
        """Get things with requests"""
        return requests.put(url, kwargs)

    def set_access_token(self): # create inputs for email and password
        ### setting access_token ###
        '''
        credentials should be a path to file containing florence login email and password
        '''
        
        with open(self.credentials, 'r') as json_file:
            credentials_json = json.load(json_file)
        
        email = credentials_json['email']
        password = credentials_json['password']
        login = {"email":email, "password":password}
        
        r = self._post(self.zebedee_url, json=login, verify=False)
        if r.status_code == 200:
            self.access_token = r.text.strip('"')
        else:
            raise Exception('Token not created, returned a {} error'.format(r.status_code))


    def get_recipe_api(self):
        ''' returns whole recipe api '''

        # Caching
        if self.recipe_api_contents:
            return self.recipe_api_contents
        
        headers = {'X-Florence-Token':self.access_token}
        r = self._get(self.recipe_api_url + f'?limit={self.recipe_api_limit}', headers=headers, verify=False)
        
        if r.status_code == 200:
            recipe_dict = r.json()
            self.recipe_api_contents = recipe_dict # store for next time
            return recipe_dict
        else:
            raise Exception('Recipe API returned a {} error'.format(r.status_code))
            
            
    def check_recipe_exists(self):
        '''
        Checks to make sure a recipe exists for dataset_id
        Returns nothing if recipe exists, an error if not
        Uses Get_Recipe_Api()
        '''
        recipe_dict = self.get_recipe_api()
        # create a list of all existing dataset ids
        dataset_id_list = []
        for item in recipe_dict['items']:
            dataset_id_list.append(item['output_instances'][0]['dataset_id'])
        if self.dataset_id not in dataset_id_list:
            raise Exception('Recipe does not exist for {}'.format(self.dataset_id))
        

    def get_recipe(self):
        ''' 
        Returns recipe for specific dataset 
        Uses Get_Recipe_Api()
        dataset_id is the dataset_id from the recipe
        '''
        self.check_recipe_exists(self.access_token, self.dataset_id)
        recipe_dict = self.get_recipe_api(self.access_token)
        # iterate through recipe api to find correct dataset_id
        for item in recipe_dict['items']:
            if self.dataset_id == item['output_instances'][0]['dataset_id']:
                return item


    def get_recipe_info(self):
        ''' 
        Returns useful recipe information for specific dataset 
        Uses Get_Recipe()
        '''
        recipe_dict = self.get_recipe()
        recipe_info_dict = {}
        recipe_info_dict['dataset_id'] = self.dataset_id
        recipe_info_dict['recipe_alias'] = recipe_dict['files'][0]['description']
        recipe_info_dict['recipe_id'] = recipe_dict['id']
        return recipe_info_dict
        
        
    def get_recipe_info_from_recipe_id(self):
        '''
        Returns useful recipe information for specific dataset
        Uses recipe_id to get recipe information
        '''
        
        recipe_api_url = f'{self.api_root}/recipes'
        single_recipe_url = recipe_api_url + '/' + self.recipe_id 
        
        headers = {'X-Florence-Token': self.access_token}
        
        r = self._get(single_recipe_url, headers=headers, verify=False)
        if r.status_code == 200:
            single_recipe_dict = r.json()
            return single_recipe_dict
        else:
            raise Exception('Get_Recipe_Info_From_Recipe_Id returned a {} error'.format(r.status_code))


    def get_dataset_instances_api(self):
        ''' 
        Returns /dataset/instances API 
        '''
        headers = {'X-Florence-Token': self.access_token}
        
        r = requests.get(self.dataset_instances_api_url + '?limit=1000', headers=headers, verify=False)
        if r.status_code == 200:
            whole_dict = r.json()
            total_count = whole_dict['total_count']
            if total_count <= 1000:
                dataset_instances_dict = r.json()['items']
            elif total_count > 1000:
                number_of_iterations = round(total_count / 1000) + 1
                offset = 0
                dataset_instances_dict = []
                for i in range(number_of_iterations):
                    new_url = f'{self.dataset_instances_api_url}/?limit={self.recipe_api_limit}&offset={offset}'
                    new_dict = self._get(new_url, headers=headers).json() # needs a catch for r.status_code !=
                    for item in new_dict['items']:
                        dataset_instances_dict.append(item)
                    offset += 1000
            return dataset_instances_dict
        else:
            raise Exception('/dataset/instances API returned a {} error'.format(r.status_code))


    def get_latest_dataset_instances(self):
        '''
        Returns latest upload id
        Uses Get_Dataset_Instances_Api()
        '''
        dataset_instances_dict = self.get_dataset_instances_api(self.access_token)
        latest_id = dataset_instances_dict['items'][0]['id']
        return latest_id


    def get_dataset_instance_info(self, instance_id):
        '''
        Return specific dataset instance info
        '''
        dataset_instances_url = f'{self.dataset_instances_api_url}/{instance_id}'
        headers = {'X-Florence-Token': self.access_token}
        
        r = self._get(dataset_instances_url, headers=headers, verify=False)
        if r.status_code == 200:
            dataset_instances_dict = r.json()
            return dataset_instances_dict
        else:
            raise Exception(f'/dataset/instances/{instance_id} API returned a {r.status_code} error')
        

    def get_dataset_jobs_api(self):
        '''
        Returns dataset/jobs API
        '''
        headers = {'X-Florence-Token': self.access_token}
        
        r = requests.get(self.dataset_jobs_api_url + '?limit=1000', headers=headers, verify=False)
        if r.status_code == 200:
            whole_dict = r.json()
            total_count = whole_dict['total_count']
            if total_count <= 1000:
                dataset_jobs_dict = whole_dict['items']
            elif total_count > 1000:
                number_of_iterations = round(total_count / 1000) + 1
                offset = 0
                dataset_jobs_dict = []
                for i in range(number_of_iterations):
                    new_url = self.dataset_jobs_api_url + f'?limit={self.dataset_jobs_api_limit}&offset={offset}'
                    new_dict = self._get(new_url, headers=headers).json()
                    for item in new_dict['items']:
                        dataset_jobs_dict.append(item)
                    offset += 1000
            return dataset_jobs_dict
        else:
            raise Exception('/dataset/jobs API returned a {} error'.format(r.status_code))
            
            
    def get_latest_job_info(self):
        '''
        Returns latest job id and recipe id and instance id
        Uses Get_Dataset_Jobs_Api()
        '''
        dataset_jobs_dict = self.get_dataset_jobs_api(self.access_token)
        latest_id = dataset_jobs_dict[-1]['id']
        recipe_id = dataset_jobs_dict[-1]['recipe'] # to be used as a quick check
        instance_id = dataset_jobs_dict[-1]['links']['instances'][0]['id']
        return latest_id, recipe_id, instance_id


    def post_new_job(self, access_token, dataset_id, s3_url):
        '''
        Creates a new job in the /dataset/jobs API
        Job is created in state 'created'
        Uses Get_Recipe_Info() to get information
        '''
        dataset_dict = self.get_recipe_info(self.access_token, dataset_id)
        headers = {'X-Florence-Token':access_token}
        
        new_job_json = {
            'recipe':dataset_dict['recipe_id'],
            'state':'created',
            'links':{},
            'files':[
                {
            'alias_name':dataset_dict['recipe_alias'],
            'url':s3_url
                }   
            ]
        }
            
        r = self._post(self.dataset_jobs_api_url, headers=headers, json=new_job_json, verify=False)
        if r.status_code == 201:
            print('Job created succefully')
        else:
            raise Exception(f'Job not created, return a {r.status_code} error')
            
        # return job ID
        job_id, job_recipe_id, job_instance_id = self.get_latest_job_info()
        
        # quick check to make sure newest job id is the correct one
        if job_recipe_id != dataset_dict['recipe_id']:
            print(f'New job recipe ID ({job_recipe_id}) does not match recipe ID used to create new job ({dataset_dict["recipe_id"]})')
        else:
            print('job_id -', job_id)
            print('dataset_instance_id -', job_instance_id)
            return job_id


    def update_state_of_job(self, job_id):
        '''
        Updates state of job from created to submitted
        once submitted import process will begin
        '''

        updating_state_of_job_url = f'{self.dataset_jobs_api_url}/{job_id}'
        headers = {'X-Florence-Token': self.access_token}

        updating_state_of_job_json = {}
        updating_state_of_job_json['state'] = 'submitted'
        
        # make sure file is in the job before continuing
        job_id_dict = self.get_job_info(self.access_token, job_id)
        
        if len(job_id_dict['files']) != 0:
            r = self._put(updating_state_of_job_url, headers=headers, json=updating_state_of_job_json, verify=False)
            if r.status_code == 200:
                print('State updated successfully')
            else:
                print(f'State not updated, return error code {r.status_code}')
        else:
            raise Exception('Job does not have a v4 file!')
        

    def get_job_info(self, access_token, job_id):
        '''
        Return job info
        '''
        dataset_jobs_id_url = f'{self.dataset_jobs_api_url}/{job_id}'
        headers = {'X-Florence-Token':access_token}
        
        r = self._get(dataset_jobs_id_url, headers=headers, verify=False)
        if r.status_code == 200:
            job_info_dict = r.json()
            return job_info_dict
        else:
            raise Exception(f'/dataset/jobs/{job_id} returned error {r.status_code}')

    def post_v4_to_s3(self):
        '''
        Uploading a v4 to the s3 bucket
        v4 is full file path
        '''
        csv_size = str(os.path.getsize(self.v4)) # Size of the csv
        timestamp = datetime.datetime.now() # to be used as unique resumableIdentifier
        timestamp = datetime.datetime.strftime(timestamp, '%d%m%y%H%M%S')
        file_name = self.v4.split("/")[-1]
        
        headers = {'X-Florence-Token': self.access_token}
        with open(self.v4, 'rb') as f:
            # Inlcude the opened file in the request
            files = {'file': f}
            # Params that can be added to the request
            # Uploading it as a single chunk of the exact size of the file in question
            params = {
                    "resumableType": "text/csv",
                    "resumableChunkNumber": 1,
                    "resumableCurrentChunkSize": csv_size,
                    "resumableTotalSize": csv_size,
                    "resumableChunkSize": csv_size,
                    "resumableIdentifier": timestamp + '-' + file_name.replace('.', ''),
                    "resumableFilename": file_name,
                    "resumableRelativePath": ".",
                    "resumableTotalChunks": 1
            }

            r = self._post(self.upload_url, headers=headers, params=params, files=files, verify=False)
            if r.status_code != 200:  #
                raise Exception(f'{self.upload_url} returned error {r.status_code}')
        
        s3_url = f'{self.base_s3_url}/{params["resumableIdentifier"]}'
        return s3_url


    def upload_data_to_florence(self):
        '''Uploads v4 to florence'''
        # get access_token
        self.set_access_token()
        
        # quick check to make sure recipe exists in API
        self.check_recipe_exists()
        
        # upload v4 into s3 bucket
        s3_url = self.post_v4_to_s3()
        
        # create new job
        job_id = self.post_new_job(s3_url)
        
        # update state of job
        self.update_state_of_job(job_id)