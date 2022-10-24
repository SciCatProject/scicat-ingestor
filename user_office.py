#! /usr/b:win/env python3
import json
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
        self._base_url = base_url + ( "/graphql" if not base_url.endswith("graphql") else "")
        self._proposal_fields = """
            primaryKey,
            proposalId,
            proposer {
                id,
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


    def set_access_token(self, token: str, error: bool = True) -> bool:
        """
        Set the access token and test that it can access

        Parameters
        ----------
        token: str
            access token
        error: bool (defaul True)
            throw error if it is not avble to connect if True

        Returns
        -------
        result: bool
            true if it could verify that it can access useroffice api

        """ 
        self._access_token = token
        self._headers = {"authorization": "Bearer " + self._access_token}

        query = """
            query {
                users {
                    totalCount
                }
            }
        """
        
        res = requests.post(
            self._base_url, 
            json={"query": query}, 
            headers=self._headers
        )

        output = ( res.status_code == 200 and "totalCount" in res.json()["data"]["users"] )
        if error and not output:
            # throw exception
            raise Exception("Unable to establish connection with User Office")

        return output
        


    def proposals_get_one_by_primary_key(self, primaryKey: int) -> dict:
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


    def proposals_get_one(self, id: str) -> dict:
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
         query {
          proposals(filter: { referenceNumbers: ["%s"] }) {
           proposals {
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

        return res.json()["data"]["proposals"]["proposals"][0]


    def users_get_one_email(self, id: str) -> dict:
        """
        Get a proposal by id.
 
        Parameters
        ----------
        id : int
            The user id
 
        Returns
        -------
        str
            The user email
        """

        query = """
         query {
          user(userId:%s) {
           email
          }
         }     
        """ % (
            id
        )
        res = requests.post(
            self._base_url,
            json={"query": query},
            headers=self._headers
        )

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()["data"]["user"]["email"]




























