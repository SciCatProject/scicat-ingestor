#! /usr/bin/env python3

import json
import sys
import requests
from urllib import parse


class SciCat:
    """
    SciCat client

    ...

    Attributes
    ----------
    base_url : str
      Base URL of the SciCat deployment

    Methods
    -------
    login(username, password):
      Sign in to SciCat and set the access token for this instance.

    get_instrument_by_name(name):
      Get an instrument by name.

    get_proposal(id):
      Get a proposal by id.

    post_dataset(dataset):
      Post a dataset to SciCat.

    post_dataset_origdatablock(pid, orig_datablock):
      Post an origdatablock related to a dataset in SciCat.
    """

    def __init__(self, base_url: str):
        self._base_url = base_url + "/api/v3"
        self._access_token = ""
        self._headers = {}

    def login(self, username: str, password: str):
        """
        Sign in to SciCat and set the access token for this instance.

        Parameters
        ----------
        username : str
          Username of a functional account
        password : str
          Password of a functional account

        Returns
        -------
        None
        """

        endpoint = "/Users/login"
        url = self._base_url + endpoint
        credentials = {"username": username, "password": password}
        res = requests.post(url, json=credentials)

        if res.status_code != 200:
            sys.exit(res.text)

        self._access_token = res.json()["id"]
        self._headers["Authorization"] = self._access_token


    def get_instrument_by_name(self, name: str) -> dict:
        """
        Get an instrument by name.

        Parameters
        ----------
        name : str
            The name of the instrument

        Returns
        -------
        dict
            The instrument with the requested name
        """

        endpoint = "/Instruments/findOne"
        query = json.dumps({"where": {"name": {"like": name}}})
        url = self._base_url + endpoint + "?" + query
        res = requests.get(url, headers=self._headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()


    def get_instrument_by_pid(self, pid: str) -> dict:
        """
        Get an instrument by pid.

        Parameters
        ----------
        pid : str
            The pid of the instrument

        Returns
        -------
        dict
            The instrument with the requested pid
        """

        encoded_pid = parse.quote_plus(pid)
        endpoint = "/Instruments/{}".format(encoded_pid)
        url = self._base_url + endpoint
        res = requests.get(url, headers=self._headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()


    def get_sample_by_pid(self, pid: str) -> dict:
        """
        Get an sample by pid.

        Parameters
        ----------
        pid : str
            The pid of the sample

        Returns
        -------
        dict
            The sample with the requested pid
        """

        encoded_pid = parse.quote_plus(pid)
        endpoint = "/Samples/{}".format(encoded_pid)
        url = self._base_url + endpoint
        res = requests.get(url, headers=self._headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()


    def get_proposal_by_pid(self, pid: str) -> dict:
        """
        Get proposal by pid.

        Parameters
        ----------
        pid : str
            The pid of the proposal

        Returns
        -------
        dict
            The proposal with the requested pid
        """

        endpoint = "/Proposals/"
        url = self._base_url + endpoint + pid
        res = requests.get(url, headers=self._headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()


    def post_dataset(self, dataset: dict) -> dict:
        """
        Post SciCat Dataset

        Parameters
        ----------
        dataset : dict
            The dataset to create

        Returns
        -------
        dict
            The created dataset with PID
        """

        endpoint = "/Datasets"
        url = self._base_url + endpoint
        res = requests.post(url, json=dataset, headers=self._headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()


    def post_dataset_orig_datablock(self, datasetPid: str, orig_datablock: dict) -> dict:
        """
        Post SciCat Dataset OrigDatablock

        Parameters
        ----------
        pid : str
            The PID of the dataset
        orig_datablock : dict
            The OrigDatablock to create

        Returns
        -------
        dict
            The created OrigDatablock with id
        """

        encoded_pid = parse.quote_plus(datasetPid)
        endpoint = "/Datasets/" + encoded_pid + "/origdatablocks"
        url = self._base_url + endpoint
        res = requests.post(url, json=orig_datablock, headers=self._headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()
