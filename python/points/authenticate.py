import os
import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
import json
from datetime import datetime

# Retrieve environment variables
FLOW_POINTS_URL = os.getenv('FLOW_POINTS_URL')
PRIVATE_KEY = os.getenv('PRIVATE_KEY')
PUBLIC_ADDRESS = os.getenv('PUBLIC_ADDRESS')

def verify_wallet_address(address):
    """
    Verifies if the provided address is a valid Ethereum address.

    Args:
        address (str): The Ethereum address to verify.

    Returns:
        bool: Returns True if valid, False otherwise.
    """
    if not Web3.is_address(address):
        print("Invalid Ethereum address format.")
        return False

    print("The wallet address is valid and correctly formatted.")
    return True

def authenticate_dapp():
    """
    Authenticates the Dapp by generating a JWT.

    Steps:
    1. Create a Dapp Challenge.
    2. Sign the challengeData using the private key.
    3. Solve the Dapp Challenge to receive a JWT.

    Returns:
        str: The JWT token.
    """
    if not PRIVATE_KEY or not PUBLIC_ADDRESS:
        print("Error: PRIVATE_KEY or PUBLIC_ADDRESS not set in environment.")
        return False

    # Verify the public address format
    if not verify_wallet_address(PUBLIC_ADDRESS):
        print("Error: Invalid PUBLIC_ADDRESS format in environment.")
        return False

    try:
        # Initialize the wallet
        wallet = Account.from_key(PRIVATE_KEY)
        print("Wallet initialized successfully.")

        # Step 1: Create Dapp Challenge
        print("Creating Dapp Challenge...")
        challenge_endpoint = f"{FLOW_POINTS_URL}/points/dapp/challenge"
        challenge_payload = {
            "addressId": PUBLIC_ADDRESS.lower(),
        }
        challenge_headers = {
            "Content-Type": "application/json"
        }

        challenge_response = requests.post(challenge_endpoint, headers=challenge_headers, json=challenge_payload)

        if challenge_response.status_code != 200:
            error_message = f"Failed to create Dapp challenge: {challenge_response.status_code} {challenge_response.reason}"
            return False

        challenge_data = challenge_response.json().get("challengeData")
        print("Dapp Challenge created successfully.")

        # Step 2: Sign the challengeData
        print("Signing challengeData...")
        message = encode_defunct(text=challenge_data)
        signed_message = Account.sign_message(message, private_key=PRIVATE_KEY)
        signature = '0x' + signed_message.signature.hex()
        print("challengeData signed successfully.")

        # Step 3: Solve Dapp Challenge to get JWT
        print("Solving Dapp Challenge...")
        solve_endpoint = f"{FLOW_POINTS_URL}/points/dapp/solve"
        solve_payload = {
            "challengeData": challenge_data,
            "signature": signature,
        }
        solve_headers = {
            "Content-Type": "application/json"
        }

        solve_response = requests.post(solve_endpoint, headers=solve_headers, json=solve_payload)

        if solve_response.status_code != 200:
            error_message = f"Failed to solve Dapp challenge: {solve_response.status_code} {solve_response.reason}"
            return False

        token = solve_response.json().get("token")
        print("JWT generated successfully.")

        # Set the JWT as an environment variable
        with open(os.environ['GITHUB_ENV'], 'a') as f:
            f.write(f"JWT={token}\n")

        return token

    except requests.exceptions.RequestException as e:
        error_message = f"HTTP Request failed: {e}"
        return False
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        return False

def main():
    """
    Main function to execute the authentication process.
    """
    authenticate_dapp()

if __name__ == "__main__":
    main()
