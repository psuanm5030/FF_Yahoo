"""
Purpose: Handle the OAuth2 for Yahoo.
Notes: This script accesses the credentials saved in 'credentials_master.yml' for your Yahoo App and requests / receives tokens which are stored in 'creds.pkl' as a sanction Client object.  Upon running this for the first time, Python will open a webbrowser window where you will need to sign in / authorize access.  The code provided needs to be input before tokens can be exchanged.
"""

import yaml
import sanction
import webbrowser
import pickle

PKL_NAME = 'creds.pkl'

class Auth(object):
    # todo add a component to check refresh time / expiry.  Limit the number of times that we need to refresh.
    def __init__(self):
        self.all_creds = self.get_creds()

        try:
            print 'Refreshing existing tokens...'
            self.yahoo_auth = self.refresh()
        except:
            print 'Couldnt refresh... please authorize...'
            self.yahoo_auth = self.access()

    def get_creds(self):
        stream = file('credentials_master.yml', 'r')
        creds = yaml.load(stream)['yahoo']
        return creds

    def access(self):
        """
        Simply connect to yahoo API to get token and store in pickle.
        :return: Returns the auth object
        """
        # instantiating a client to process OAuth2 response
        c = sanction.Client(auth_endpoint=self.all_creds['auth_uri'],
                     token_endpoint=self.all_creds['token_uri'],
                     client_id=self.all_creds['consumer_key'],
                     client_secret=self.all_creds['consumer_secret'])

        # Authorize and get the token
        auth_uri_r = c.auth_uri(redirect_uri=self.all_creds['callback_url'])
        webbrowser.open(auth_uri_r)
        auth_code = raw_input('Enter the auth code: ')

        # Grab the Token
        c.request_token(redirect_uri='oob', code=auth_code)

        # Store the Token Details
        print 'Storing tokens in {}...'.format(PKL_NAME)
        with open(PKL_NAME, 'wb') as f:
            pickle.dump(c, f)

        return c

    def refresh(self):
        """
        Refresh the available authorization object (stored in a pickle)
        :return: Returns the auth object
        """
        # Load the Pickle
        with open(PKL_NAME, 'rb') as f:  # Load the pickle
            c = pickle.load(f)

        c.request_token(grant_type='refresh_token', refresh_token=c.refresh_token)
        return c