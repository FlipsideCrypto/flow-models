import os
import sys
import requests
from eth_account import Account
from eth_account.messages import encode_defunct
from web3 import Web3
import json
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

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
        logger.error("Invalid Ethereum address format.")
        return False

    logger.info("The wallet address is valid and correctly formatted.")
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
        logger.error("Error: PRIVATE_KEY or PUBLIC_ADDRESS not set in environment.")
        return False

    # Verify the public address format
    if not verify_wallet_address(PUBLIC_ADDRESS):
        logger.error("Error: Invalid PUBLIC_ADDRESS format in environment.")
        return False

    try:
        # Validate private key by attempting to create a wallet
        _ = Account.from_key(PRIVATE_KEY)
        logger.info("Wallet initialized successfully.")

        # Step 1: Create Dapp Challenge
        logger.info("Creating Dapp Challenge...")
        challenge_endpoint = f"{FLOW_POINTS_URL}/points/dapp/challenge"
        challenge_payload = {
            "addressId": PUBLIC_ADDRESS.lower(),
        }
        headers = {
            "Content-Type": "application/json"
        }

        challenge_response = requests.post(challenge_endpoint, headers=headers, json=challenge_payload)

        if challenge_response.status_code != 200:
            error_message = f"Failed to create Dapp challenge: {challenge_response.status_code} {challenge_response.reason}"
            logger.error(error_message)
            return False

        challenge_data = challenge_response.json().get("challengeData")
        logger.info("Dapp Challenge created successfully.")

        # Step 2: Sign the challengeData
        logger.info("Signing challengeData...")
        message = encode_defunct(text=challenge_data)
        signed_message = Account.sign_message(message, private_key=PRIVATE_KEY)
        signature = f"0x{signed_message.signature.hex()}"
        logger.info("challengeData signed successfully.")

        # Step 3: Solve Dapp Challenge to get JWT
        logger.info("Solving Dapp Challenge...")
        solve_endpoint = f"{FLOW_POINTS_URL}/points/dapp/solve"
        solve_payload = {
            "challengeData": challenge_data,
            "signature": signature,
        }

        solve_response = requests.post(solve_endpoint, headers=headers, json=solve_payload)

        if solve_response.status_code != 200:
            error_message = f"Failed to solve Dapp challenge: {solve_response.status_code} {solve_response.reason}"
            logger.error(error_message)
            return False

        token = solve_response.json().get("token")
        logger.info("JWT generated successfully.")

        # Set the JWT as an environment variable
        with open(os.environ['GITHUB_ENV'], 'a') as f:
            f.write(f"JWT={token}\n")

        return True

    except requests.exceptions.RequestException as e:
        error_message = f"HTTP Request failed: {e}"
        logger.error(error_message)
        return False
    except Exception as e:
        error_message = f"An unexpected error occurred: {e}"
        logger.error(error_message)
        return False

def main():
    """
    Main function to execute the authentication process.
    """
    if not authenticate_dapp():
        logger.error("Authentication failed. Exiting with status code 1.")
        sys.exit(1)
    logger.info("Authentication succeeded.")

if __name__ == "__main__":
    main()
