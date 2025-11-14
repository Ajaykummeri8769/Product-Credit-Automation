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
def childsr_handler(request):
    """HTTP Cloud function to update case status to completed"""
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "No request data provided"}, 400
        
        case_id = request_json.get('case_id') or request_json.get('caseId')
        if not case_id:
            return {"error": "No case_id provided"}, 400
        
        # Get OAuth token
        token_response = get_oauth_token()
        access_token = token_response['access_token']
        
        # Update case status to completed
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        sf_payload = {
            "Status": "Completed"
        }
        
        case_url = f"{os.getenv('GATEWAY_URL')}/system/customer-relationship-management/v3/sobjects/Case/{case_id}"
        sf_response = requests.patch(case_url, headers=headers, json=sf_payload)
        
        if sf_response.status_code == 200:
            return {"success": True, "case_id": case_id, "status": "Completed"}
        else:
            return {"error": f"Failed to update case: {sf_response.text}"}, sf_response.status_code
            
    except Exception as e:
        return {"error": str(e)}, 500