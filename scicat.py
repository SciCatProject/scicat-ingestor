#! /usr/bin/env python3

import json
import sys
import requests


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
    """

    def __init__(self, base_url: str):
        self.base_url = base_url + "/api/v3"
        self.access_token = ""

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
        url = self.base_url + endpoint
        credentials = {"username": username, "password": password}
        res = requests.post(url, json=credentials)

        if res.status_code != 200:
            sys.exit(res.text)

        self.access_token = res.json()["id"]

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
        query = json.dumps({"where": {"name": name}})
        url = self.base_url + endpoint + "?" + query
        params = {"access_token": self.access_token}
        res = requests.get(url, params=params)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()
