#! /usr/bin/env python3

import sys
import requests


class UserOffice:
    """
    UserOffice Client

    ...

    Attributes
    ----------
    None

    Methods
    -------
    login(email, password):
        Sign in to UserOffice and set the access token for this instance.

    get_proposal(id):
        Get a proposal by id.
    """

    __url = "https://useroffice-test.esss.lu.se/graphql"

    def __init__(self):
        self.__access_token = ""

    def login(self, email: str, password: str):
        """
        Sign in to UserOffice and set the access token for this instance.

        Parameters
        ----------
        email : str
            User email address
        password : str
            User password

        Returns
        -------
        None
        """

        query = """
            mutation UserMutations {
                login(email: "%s", password: "%s") {
                    token,
                    error
                }
            }
        """ % (
            email,
            password,
        )
        res = requests.post(self.__url, json={"query": query})
        if res.status_code != 200:
            sys.exit(res.text)

        access_token = res.json()["data"]["login"]["token"]
        self.__access_token = access_token

    def get_proposal(self, id: int) -> dict:
        """
        Get a proposal by id.

        Parameters
        ----------
        id : int
            The proposal id

        Returns
        -------
        dict
            The proposal with requested id
        """

        headers = {"Authorization": "Bearer " + self.__access_token}
        query = """
            query Proposals {
                proposal(id: %d) {
                    id, title, abstract, shortCode,
                    users {
                        firstname,
                        lastname,
                        organisation,
                        position
                    },
                    proposer {
                        firstname,
                        lastname,
                        organisation,
                        position
                    },
                    instrument {
                        name,
                        shortCode,
                        description
                    }
                }
            }
        """ % (
            id
        )
        res = requests.post(self.__url, json={"query": query}, headers=headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()["data"]["proposal"]

    def get_samples_by_proposalId(self, proposalId: int) -> list:
        """
        Get samples by proposal id.

        Parameters
        ----------
        proposalId : int
            Id of the proposal you want to get samples for

        Returns
        -------
        list
            List of samples matching the query
        """

        headers = {"Authorization": "Bearer " + self.__access_token}
        query = """
            query Samples {
                samples(filter: {proposalId: %d}) {
                    id, title
                }
            }
        """ % (
            proposalId
        )
        res = requests.post(self.__url, json={"query": query}, headers=headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()["data"]["samples"]
