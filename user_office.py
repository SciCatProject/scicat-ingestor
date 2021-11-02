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
        self.access_token = ""
        self.base_url = base_url + "/graphql"

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
        res = requests.post(self.base_url, json={"query": query})
        if res.status_code != 200:
            sys.exit(res.text)

        access_token = res.json()["data"]["login"]["token"]
        self.access_token = access_token

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

        headers = {"Authorization": "Bearer " + self.access_token}
        query = """
            query Proposals {
                proposal(primaryKey: %d) {
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
                }
            }
        """ % (
            id
        )
        res = requests.post(self.base_url, json={"query": query}, headers=headers)

        if res.status_code != 200:
            sys.exit(res.text)

        return res.json()["data"]["proposal"]
