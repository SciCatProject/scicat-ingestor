import os
import logging
import requests
import json
from typing import List, Dict, Any
from scicat_offline_ingestor import build_offline_config, build_logger
from scicat_communication import get_proposals, update_published_status
from scicat_ingestor_utils import login, logout, is_connection_error

def get_public_proposals(logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Retrieve public proposals from SciCat API.
    
    Args:
        logger: Logger instance
        
    Returns:
        List of proposal dictionaries
    """
    token = os.environ.get("TOKEN_ILL")
    if not token:
        logger.error("TOKEN_ILL environment variable not set")
        raise ValueError("TOKEN_ILL environment variable is required")
    
    endpoint = "https://webserv-dev.ill.fr/api/scicat/proposals/public"
    
    headers = {
        "SCICAT-AUTH-TOKEN": token,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        
        proposals = response.json()
        logger.info(f"Retrieved {len(proposals)} public proposals")
        
        return proposals
    
    except requests.RequestException as e:
        logger.error(f"Error retrieving public proposals: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing response JSON: {e}")
        raise

def extract_unique_proposal_ids(proposals: List[Dict[str, Any]]) -> List[int]:
    """
    Extract unique proposal IDs from proposal data.
    
    Args:
        proposals: List of proposal dictionaries
        
    Returns:
        List of unique proposal IDs
    """
    unique_ids = []
    seen = set()
    
    for proposal in proposals:
        proposal_id = proposal.get("proposalNumber")
        if proposal_id and proposal_id not in seen:
            seen.add(proposal_id)
            unique_ids.append(proposal_id)
    
    return unique_ids

def main() -> List[int]:
    """
    Main function to retrieve public proposal IDs.
    
    Returns:
        List of unique proposal IDs
    """
    tmp_config = build_offline_config()
    logger = build_logger(tmp_config)
    config = build_offline_config(logger=logger)
    
    try:
        proposals = get_public_proposals(logger)
        
        public_proposal_ids = extract_unique_proposal_ids(proposals)
        logger.info(f"Found {len(public_proposal_ids)} unique proposal IDs")

        try:
            config.scicat.token = login(config.scicat, logger)

            scicat_proposals = get_proposals(config.scicat, logger)
            scicat_proposal_ids = [p["proposalId"] for p in scicat_proposals]
            logger.info(f"Found {len(scicat_proposal_ids)} proposals in SciCat")

            update_published_status(set(scicat_proposal_ids) - set(public_proposal_ids), False, config.scicat, logger)
            update_published_status(set(scicat_proposal_ids) & set(public_proposal_ids), True, config.scicat, logger)

            logger.info(f"Updated published status for {len(public_proposal_ids)} proposals")
        except Exception as e:
            if is_connection_error(e):
                logger.error("Connection error when contacting SciCat server: %s", str(e))
                raise ConnectionError("Cannot connect to SciCat server") from e
            raise

        try:
            logout(config.scicat, logger)
        except Exception as e:
            pass
    
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return []

if __name__ == "__main__":
    main()