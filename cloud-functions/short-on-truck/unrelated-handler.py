import os
import requests
import functions_framework
from dotenv import load_dotenv
load_dotenv()

def get_oauth_token():
    """Get OAuth token using client credentials via API gateway"""
    token_url = f"{os.getenv('GATEWAY_URL')}/token"
    
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'client_credentials',
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET')
    }
    
    response = requests.post(token_url, headers=headers, data=data)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to get token: {response.text}")

@functions_framework.http
def unrelated_handler(request):
    """HTTP Cloud function to handle unrelated/unsure triage cases"""
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "No request data provided"}, 400
        
        case_id = request_json.get('case_id')
        triage_response = request_json.get('triage_response', {})
        
        if not case_id:
            return {"error": "No case_id provided"}, 400
        
        # Get OAuth token
        token_response = get_oauth_token()
        access_token = token_response['access_token']
        
        # Extract intent from triage response - handle both structures
        agent_response = triage_response.get('agent_response', [])
        if isinstance(agent_response, list) and len(agent_response) > 0:
            intent = agent_response[0].get('intent', 'Unknown')
        else:
            intent = 'Unknown'
        
        # Update case with unrelated owner and notes
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        sbs_notes = f"Triage Analysis: Request not related to credit/refund\nIntent: {intent}\nRouted to human queue for manual review"
        
        sf_payload = {
            "OwnerId": os.environ.get('UNRELATED_OWNER_ID', '00G8b000003nMZdEAM'),
            "SBS_Notes__c": sbs_notes
        }
        
        case_url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Case/{case_id}"
        sf_response = requests.patch(case_url, headers=headers, json=sf_payload)
        
        if sf_response.status_code == 200:
            return {
                "status": "routed_to_human",
                "case_id": case_id,
                "intent": intent,
                "message": "Case routed to human queue"
            }
        else:
            return {"error": f"Failed to update case: {sf_response.text}"}, sf_response.status_code
            
    except Exception as e:
        return {"error": str(e)}, 500