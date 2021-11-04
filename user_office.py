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

    def __init__(self, base_url: str):
        self._access_token = ""
        self._headers = {}
        self._base_url = base_url + "/graphql"
        self._proposal_fields = """
            primaryKey,
            proposalId,
            proposer {
                firstname,
                lastname,
                organisation,
                position
            },
            instrument {
                id,
                name,
                shortCode,
                description
            }
        """


    def login(self, email: str, password: str) -> None:
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
                    rejection {
                      reason,
                      context,
                      exception
                    }
                }
            }
        """ % (
            email,
            password,
        )
        res = requests.post(
            self._base_url, 
            json={"query": query}
        )
        if res.status_code != 200:
            sys.exit(res.text)

        access_token = res.json()["data"]["login"]["token"]
        self._access_token = access_token
        self._headers = {"Authorization": "Bearer " + self._access_token}



    def get_proposal_by_primary_key(self, primaryKey: int) -> dict:
        """
        Get a proposal by primary key.

        Parameters
        ----------
        primaryKey : int
            The proposal primary key

        Returns
        -------
        dict
            The proposal with requested primary key
        """

        query = """
            query Proposals {
                proposal(primaryKey: %d) {
                    %s
                }
            }
        """ % (
            primaryKey, 
            self._proposal_fields
        )
        res = requests.post(
            self._base_url, 
            json={"query": query}, 
            headers=self._headers
        )

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()["data"]["proposal"]


    def get_proposal_by_id(self, id: int) -> dict:
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

        query = """
            {
                proposals(filter:{ shortCodes : ["%d"]}) {
                    proposals{
                        %s
                    }
                }
            }
        """ % (
            id,
            self._proposal_fields
        )
        res = requests.post(
            self._base_url, 
            json={"query": query}, 
            headers=self._headers
        )

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()["data"]["proposals"]
